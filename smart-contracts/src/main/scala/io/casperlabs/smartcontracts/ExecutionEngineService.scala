package io.casperlabs.smartcontracts

import java.nio.file.Path

import cats.effect.{Resource, Sync}
import cats.implicits._
import cats.Defer
import com.google.protobuf.ByteString
import io.casperlabs.casper.consensus.Bond
import io.casperlabs.crypto.Keys.PublicKey
import io.casperlabs.crypto.codec.Base16
import io.casperlabs.casper.consensus.state.{Unit => SUnit, _}
import io.casperlabs.ipc._
import io.casperlabs.metrics.Metrics
import io.casperlabs.models.SmartContractEngineError
import io.casperlabs.shared.Log
import io.casperlabs.smartcontracts.ExecutionEngineService.Stub
import monix.eval.{Task, TaskLift}
import simulacrum.typeclass
import scala.util.{Either, Try}
import io.casperlabs.catscontrib.MonadThrowable
import io.casperlabs.ipc.ChainSpec.{GenesisConfig, UpgradePoint}
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol

@typeclass trait ExecutionEngineService[F[_]] {
  def emptyStateHash: ByteString
  def runGenesis(
      genesisConfig: GenesisConfig
  ): F[Either[Throwable, GenesisResult]]
  def upgrade(
      prestate: ByteString,
      upgrade: UpgradePoint,
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, UpgradeResult]]
  def exec(
      prestate: ByteString,
      blocktime: Long,
      deploys: Seq[DeployItem],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, Seq[DeployResult]]]
  def commit(
      prestate: ByteString,
      effects: Seq[TransformEntry],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, ExecutionEngineService.CommitResult]]
  def query(
      state: ByteString,
      baseKey: Key,
      path: Seq[String],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, Value]]
}

class GrpcExecutionEngineService[F[_]: Defer: Sync: Log: TaskLift: Metrics] private[smartcontracts] (
    addr: Path,
    stub: Stub,
    messageSizeLimit: Int
) extends ExecutionEngineService[F] {
  import GrpcExecutionEngineService.EngineMetricsSource

  override def emptyStateHash: ByteString = {
    val arr: Array[Byte] = Array(
      51, 7, 165, 76, 166, 213, 191, 186, 252, 14, 241, 176, 3, 243, 236, 73, 65, 192, 17, 238, 127,
      121, 136, 158, 68, 65, 103, 84, 222, 47, 9, 29
    ).map(_.toByte)
    ByteString.copyFrom(arr)
  }

  private def sendMessage[A, B, R](msg: A, rpc: Stub => A => Task[B])(f: B => R): F[R] =
    rpc(stub)(msg).to[F].map(f(_)).recoverWith {
      case ex: io.grpc.StatusRuntimeException
          if ex.getStatus.getCode == io.grpc.Status.Code.UNAVAILABLE &&
            ex.getCause != null &&
            ex.getCause.isInstanceOf[java.io.FileNotFoundException] =>
        Sync[F].raiseError(
          new java.io.FileNotFoundException(
            s"It looks like the Execution Engine is not listening at the socket file ${addr}"
          )
        )
    }

  override def exec(
      prestate: ByteString,
      blocktime: Long,
      deploys: Seq[DeployItem],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, Seq[DeployResult]]] = Metrics[F].timer("eeExec") {
    val baseExecRequest =
      ExecuteRequest(prestate, blocktime, protocolVersion = Some(protocolVersion))
    // Build batches limited by the size of message sent to EE.
    val batches =
      ExecutionEngineService.batchDeploysBySize(baseExecRequest, messageSizeLimit)(deploys)

    for {
      result <- batches.traverse { request =>
                 sendMessage(request, _.execute) {
                   _.result match {
                     case ExecuteResponse.Result.Success(ExecResult(deployResults)) =>
                       Right(deployResults) //TODO: Capture errors better than just as a string
                     case ExecuteResponse.Result.Empty =>
                       Left(new SmartContractEngineError("empty response"))
                     case ExecuteResponse.Result.MissingParent(RootNotFound(missing)) =>
                       Left(
                         new SmartContractEngineError(
                           s"Missing states: ${Base16.encode(missing.toByteArray)}"
                         )
                       )
                   }
                 }
               } map { _.sequence.map(_.flatten) }
      _ <- result.fold(
            _ => ().pure[F],
            deployResults => {
              // XXX: EE returns cost as BigInt but metrics are in Long. In practice it will be unlikely exhaust the limits of Long.
              val gasSpent =
                deployResults.foldLeft(0L)(
                  (a, d) => a + d.value.executionResult.fold(0L)(_.cost.fold(0L)(_.value.toLong))
                )
              Metrics[F].incrementCounter("gas_spent", gasSpent)
            }
          )
    } yield result
  }

  override def runGenesis(
      genesisConfig: GenesisConfig
  ): F[Either[Throwable, GenesisResult]] =
    sendMessage(genesisConfig, _.runGenesis) {
      _.result match {
        case GenesisResponse.Result.Success(result) =>
          Right(result)
        case GenesisResponse.Result.FailedDeploy(error) =>
          Left(new SmartContractEngineError(error.message))
        case GenesisResponse.Result.Empty =>
          Left(new SmartContractEngineError("empty response"))
      }
    }

  def upgrade(
      prestate: ByteString,
      upgrade: UpgradePoint,
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, UpgradeResult]] =
    sendMessage(
      UpgradeRequest(prestate, Some(upgrade), Some(protocolVersion)),
      _.upgrade
    ) {
      _.result match {
        case UpgradeResponse.Result.Success(result) =>
          Right(result)
        case UpgradeResponse.Result.FailedDeploy(error) =>
          Left(new SmartContractEngineError(error.message))
        case UpgradeResponse.Result.Empty =>
          Left(new SmartContractEngineError("empty response"))
      }
    }

  override def commit(
      prestate: ByteString,
      effects: Seq[TransformEntry],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, ExecutionEngineService.CommitResult]] =
    Metrics[F].timer("eeCommit") {
      sendMessage(CommitRequest(prestate, effects, Some(protocolVersion)), _.commit) {
        _.result match {
          case CommitResponse.Result.Success(commitResult) =>
            Right(ExecutionEngineService.CommitResult(commitResult))
          case CommitResponse.Result.Empty => Left(SmartContractEngineError("empty response"))
          case CommitResponse.Result.MissingPrestate(RootNotFound(hash)) =>
            Left(SmartContractEngineError(s"Missing pre-state: ${Base16.encode(hash.toByteArray)}"))
          case CommitResponse.Result.FailedTransform(PostEffectsError(message)) =>
            Left(SmartContractEngineError(s"Error executing transform: $message"))
          case CommitResponse.Result.KeyNotFound(value) =>
            Left(SmartContractEngineError(s"Key not found in global state: $value"))
          case CommitResponse.Result.TypeMismatch(err) =>
            Left(SmartContractEngineError(err.toString))
        }
      }
    }

  override def query(
      state: ByteString,
      baseKey: Key,
      path: Seq[String],
      protocolVersion: ProtocolVersion
  ): F[Either[Throwable, Value]] =
    sendMessage(QueryRequest(state, Some(baseKey), path, Some(protocolVersion)), _.query) {
      _.result match {
        case QueryResponse.Result.Success(value) => Right(value)
        case QueryResponse.Result.Empty          => Left(SmartContractEngineError("empty response"))
        case QueryResponse.Result.Failure(err)   => Left(SmartContractEngineError(err))
      }
    }
}

object ExecutionEngineService {
  type Stub = IpcGrpcMonix.ExecutionEngineServiceStub

  class CommitResult private (val postStateHash: ByteString, val bondedValidators: Seq[Bond])

  object CommitResult {
    def apply(ipcCommitResult: io.casperlabs.ipc.CommitResult): CommitResult = {
      // XXX: EE returns bonds as BigInt but we treat it as Long.
      val validators = ipcCommitResult.bondedValidators.map(
        b => Bond(b.validatorPublicKey, b.stake)
      )
      new CommitResult(ipcCommitResult.poststateHash, validators)
    }

    def apply(postStateHash: ByteString, bonds: Seq[Bond]): CommitResult =
      new CommitResult(postStateHash, bonds)
  }

  def batchElements[A](
      deploys: Seq[A],
      canAdd: (List[A], A) => Boolean
  ): List[List[A]] =
    deploys
      .foldRight(List.empty[List[A]]) {
        case (item, Nil) => List(item) :: Nil
        case (item, hd :: tail) =>
          if (canAdd(hd, item))
            (item :: hd) :: tail
          else
            List(item) :: hd :: tail
      }

  def batchDeploysBySize(base: ExecuteRequest, messageSizeLimit: Int)(
      deploys: Seq[DeployItem]
  ): List[ExecuteRequest] = {
    val test: (List[DeployItem], DeployItem) => Boolean =
      (batch, item) =>
        base
          .withDeploys(item :: batch)
          .serializedSize <= messageSizeLimit

    batchElements(deploys, test)
      .map(batch => base.withDeploys(batch))
  }

}

object GrpcExecutionEngineService {
  implicit val EngineMetricsSource: Metrics.Source =
    Metrics.Source(Metrics.BaseSource, "engine")

  private def initializeMetrics[F[_]: Metrics] =
    Metrics[F].incrementCounter("gas_spent", 0)

  def apply[F[_]: Sync: Log: TaskLift: Metrics](
      addr: Path,
      maxMessageSize: Int
  ): Resource[F, GrpcExecutionEngineService[F]] =
    for {
      service <- new ExecutionEngineConf[F](addr, maxMessageSize).apply
      _       <- Resource.liftF(initializeMetrics)
    } yield service
}
