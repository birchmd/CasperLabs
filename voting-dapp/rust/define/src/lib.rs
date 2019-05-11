#![no_std]
#![feature(alloc)]

extern crate alloc;
use alloc::collections::BTreeMap;
use alloc::prelude::ToString;
use alloc::string::String;

extern crate common;
use common::contract_api::pointers::UPointer;
use common::contract_api::{add, add_uref, get_arg, get_uref, new_uref, read, store_function};
use common::key::Key;

#[no_mangle]
pub extern "C" fn vote_ext() {
    let num_options_key: UPointer<i32> = get_uref("num_options").to_u_ptr().unwrap();
    let num_options = read(num_options_key);
    let vote_option: i32 = get_arg(0);
    if vote_option >= 0 && vote_option < num_options {
        let vote_count_key: UPointer<i32> = get_uref(&vote_option.to_string()).to_u_ptr().unwrap();
        add(vote_count_key, 1);
    }
}

#[no_mangle]
pub extern "C" fn call() {
    let num_options: i32 = 7;

    let mut name_map: BTreeMap<String, Key> = BTreeMap::new();

    let num_options_key: Key = new_uref(num_options).into();
    name_map.insert(String::from("num_options"), num_options_key);
    for i in 0..num_options {
        let key: UPointer<i32> = new_uref(0);
        name_map.insert(i.to_string(), key.into());
    }

    let hash = store_function("vote_ext", name_map);
    add_uref("vote", &hash.into());
}
