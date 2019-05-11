#![no_std]
#![feature(alloc)]

extern crate alloc;
use alloc::vec::Vec;

extern crate common;
use common::contract_api::pointers::ContractPointer;
use common::contract_api::{call_contract, get_arg};

#[no_mangle]
pub extern "C" fn call() {
    let hash = ContractPointer::Hash([
        164, 102, 153, 51, 236, 214, 169, 167, 126, 44, 250, 247, 179, 214, 203, 229, 239, 69, 145,
        25, 5, 153, 113, 55, 255, 188, 176, 201, 7, 4, 42, 100,
    ]);
    let vote: i32 = get_arg(0);
    let _: () = call_contract(hash, &vote, &Vec::new());
}
