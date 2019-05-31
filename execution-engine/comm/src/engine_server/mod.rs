use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::marker::{Send, Sync};

use common::key::Key;
use execution_engine::engine::{EngineState, Error as EngineError};
use execution_engine::execution::{Executor, WasmiExecutor};
use execution_engine::trackingcopy::QueryResult;
use ipc::*;
use ipc_grpc::ExecutionEngineService;
use mappings::*;
use shared::newtypes::Blake2bHash;
use storage::history::*;
use storage::transform::Transform;
use wasm_prep::{Preprocessor, WasmiPreprocessor};

pub mod ipc;
pub mod ipc_grpc;
pub mod mappings;

// Idea is that Engine will represent the core of the execution engine project.
// It will act as an entry point for execution of Wasm binaries.
// Proto definitions should be translated into domain objects when Engine's API is invoked.
// This way core won't depend on comm (outer layer) leading to cleaner design.
impl<H> ipc_grpc::ExecutionEngineService for EngineState<H>
where
    H: History,
    EngineError: From<H::Error>,
    H::Error: Into<execution_engine::execution::Error> + Debug,
{
    fn query(
        &self,
        _o: ::grpc::RequestOptions,
        p: ipc::QueryRequest,
    ) -> grpc::SingleResponse<ipc::QueryResponse> {
        // TODO: don't unwrap
        let state_hash: Blake2bHash = p.get_state_hash().try_into().unwrap();
        match p.get_base_key().try_into() {
            Err(ParsingError(err_msg)) => {
                let mut result = ipc::QueryResponse::new();
                result.set_failure(err_msg);
                grpc::SingleResponse::completed(result)
            }
            Ok(key) => {
                let path = p.get_path();
                match self.tracking_copy(state_hash) {
                    Err(storage_error) => {
                        let mut result = ipc::QueryResponse::new();
                        let error = format!("Error during checkout out Trie: {:?}", storage_error);
                        result.set_failure(error);
                        grpc::SingleResponse::completed(result)
                    }
                    Ok(None) => {
                        let mut result = ipc::QueryResponse::new();
                        let error = format!("Root not found: {:?}", state_hash);
                        result.set_failure(error);
                        grpc::SingleResponse::completed(result)
                    }
                    Ok(Some(mut tc)) => {
                        let response = match tc.query(key, path) {
                            Err(err) => {
                                let mut result = ipc::QueryResponse::new();
                                let error = format!("{:?}", err);
                                result.set_failure(error);
                                result
                            }

                            Ok(QueryResult::ValueNotFound(full_path)) => {
                                let mut result = ipc::QueryResponse::new();
                                let error = format!("Value not found: {:?}", full_path);
                                result.set_failure(error);
                                result
                            }

                            Ok(QueryResult::Success(value)) => {
                                let mut result = ipc::QueryResponse::new();
                                result.set_success(value.into());
                                result
                            }
                        };
                        grpc::SingleResponse::completed(response)
                    }
                }
            }
        }
    }

    fn exec(
        &self,
        _o: ::grpc::RequestOptions,
        p: ipc::ExecRequest,
    ) -> grpc::SingleResponse<ipc::ExecResponse> {
        let executor = WasmiExecutor;
        // TODO(mateusz.gorski): Use `protocol_version` and `WasmiPreprocessor::from_protocol_version`.
        let preprocessor: WasmiPreprocessor = Default::default();
        // TODO: don't unwrap
        let prestate_hash: Blake2bHash = p.get_parent_state_hash().try_into().unwrap();
        let deploys = p.get_deploys();
        let protocol_version = p.get_protocol_version();
        let deploys_result: Result<Vec<DeployResult>, RootNotFound> = run_deploys(
            &self,
            &executor,
            &preprocessor,
            prestate_hash,
            deploys,
            protocol_version,
        );
        match deploys_result {
            Ok(deploy_results) => {
                let mut exec_response = ipc::ExecResponse::new();
                let mut exec_result = ipc::ExecResult::new();
                exec_result.set_deploy_results(protobuf::RepeatedField::from_vec(deploy_results));
                exec_response.set_success(exec_result);
                grpc::SingleResponse::completed(exec_response)
            }
            Err(error) => {
                let mut exec_response = ipc::ExecResponse::new();
                exec_response.set_missing_parent(error);
                grpc::SingleResponse::completed(exec_response)
            }
        }
    }

    fn commit(
        &self,
        _o: ::grpc::RequestOptions,
        p: ipc::CommitRequest,
    ) -> grpc::SingleResponse<ipc::CommitResponse> {
        // TODO: don't unwrap
        let prestate_hash: Blake2bHash = p.get_prestate_hash().try_into().unwrap();
        let effects_seq_result: Result<Vec<(Key, Transform)>, ParsingError> =
            p.get_effects().iter().map(TryInto::try_into).collect();
        let effects_map_result: Result<HashMap<Key, Transform>, ParsingError> = effects_seq_result
            .map(|vec| {
                let mut map = HashMap::new();
                for (k, v) in vec {
                    execution_engine::utils::add(&mut map, k, v);
                }
                map
            });
        match effects_map_result {
            Err(ParsingError(error_message)) => {
                let mut res = ipc::CommitResponse::new();
                let mut err = ipc::PostEffectsError::new();
                err.set_message(error_message);
                res.set_failed_transform(err);
                grpc::SingleResponse::completed(res)
            }
            Ok(effects) => {
                let result = grpc_response_from_commit_result::<H>(
                    prestate_hash,
                    self.apply_effect(prestate_hash, effects),
                );
                grpc::SingleResponse::completed(result)
            }
        }
    }

    fn validate(
        &self,
        _o: ::grpc::RequestOptions,
        p: ValidateRequest,
    ) -> grpc::SingleResponse<ValidateResponse> {
        let pay_mod =
            wabt::Module::read_binary(p.payment_code, &wabt::ReadBinaryOptions::default())
                .and_then(|x| x.validate());
        let ses_mod =
            wabt::Module::read_binary(p.session_code, &wabt::ReadBinaryOptions::default())
                .and_then(|x| x.validate());

        match pay_mod.and(ses_mod) {
            Ok(_) => {
                let mut result = ValidateResponse::new();
                result.set_success(ValidateResponse_ValidateSuccess::new());
                grpc::SingleResponse::completed(result)
            }
            Err(cause) => {
                let mut result = ValidateResponse::new();
                result.set_failure(cause.to_string());
                grpc::SingleResponse::completed(result)
            }
        }
    }
}

fn run_deploys<A, H, E, P>(
    engine_state: &EngineState<H>,
    executor: &E,
    preprocessor: &P,
    prestate_hash: Blake2bHash,
    deploys: &[ipc::Deploy],
    protocol_version: &ProtocolVersion,
) -> Result<Vec<DeployResult>, RootNotFound>
where
    H: History,
    E: Executor<A>,
    P: Preprocessor<A>,
    EngineError: From<H::Error>,
    H::Error: Into<execution_engine::execution::Error>,
{
    // We want to treat RootNotFound error differently b/c it should short-circuit
    // the execution of ALL deploys within the block. This is because all of them share
    // the same prestate and all of them would fail.
    // Iterator (Result<_, _> + collect()) will short circuit the execution
    // when run_deploy returns Err.
    deploys
        .iter()
        .map(|deploy| {
            let session_contract = deploy.get_session();
            let module_bytes = &session_contract.code;
            let args = &session_contract.args;
            let address: [u8; 20] = {
                let mut tmp = [0u8; 20];
                tmp.copy_from_slice(&deploy.address);
                tmp
            };
            let timestamp = deploy.timestamp;
            let nonce = deploy.nonce;
            let gas_limit = deploy.gas_limit as u64;
            engine_state
                .run_deploy(
                    module_bytes,
                    args,
                    address,
                    timestamp,
                    nonce,
                    prestate_hash,
                    gas_limit,
                    protocol_version.get_version(),
                    executor,
                    preprocessor,
                )
                .map(Into::into)
                .map_err(Into::into)
        })
        .collect()
}

// Helper method which returns single DeployResult that is set to be a WasmError.
pub fn new<E: ExecutionEngineService + Sync + Send + 'static>(
    socket: &str,
    e: E,
) -> grpc::ServerBuilder {
    let socket_path = std::path::Path::new(socket);
    if socket_path.exists() {
        std::fs::remove_file(socket_path).expect("Remove old socket file.");
    }

    let mut server = grpc::ServerBuilder::new_plain();
    server.http.set_unix_addr(socket.to_owned()).unwrap();
    server.http.set_cpu_pool_threads(1);
    server.add_service(ipc_grpc::ExecutionEngineServiceServer::new_service_def(e));
    server
}
