use super::TURef;
use crate::{key::Key, value::Contract};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractRef {
    Hash([u8; 32]),
    TURef(TURef<Contract>),
}

impl ContractRef {
    pub fn into_turef(self) -> Option<TURef<Contract>> {
        match self {
            ContractRef::TURef(ret) => Some(ret),
            _ => None,
        }
    }
}

impl From<ContractRef> for Key {
    fn from(c_ptr: ContractRef) -> Self {
        match c_ptr {
            ContractRef::Hash(h) => Key::Hash(h),
            ContractRef::TURef(turef) => turef.into(),
        }
    }
}
