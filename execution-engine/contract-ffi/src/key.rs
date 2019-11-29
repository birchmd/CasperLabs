use alloc::{format, vec::Vec};

use base16;
use blake2::{
    digest::{Input, VariableOutput},
    VarBlake2b,
};
use hex_fmt::HexFmt;

use crate::{
    bytesrepr::{Error, FromBytes, ToBytes, N32, U32_SIZE},
    contract_api::{ContractRef, TURef},
    uref::{AccessRights, URef, UREF_SIZE_SERIALIZED},
};

const ACCOUNT_ID: u8 = 0;
const HASH_ID: u8 = 1;
const UREF_ID: u8 = 2;
const LOCAL_ID: u8 = 3;

pub const HASH_SIZE: usize = 32;
pub const LOCAL_KEY_SIZE: usize = 32;
pub const LOCAL_SEED_SIZE: usize = 32;

const KEY_ID_SIZE: usize = 1; // u8 used to determine the ID
const ACCOUNT_KEY_SIZE: usize = KEY_ID_SIZE + U32_SIZE + N32;
const HASH_KEY_SIZE: usize = KEY_ID_SIZE + U32_SIZE + N32;
pub const UREF_SIZE: usize = KEY_ID_SIZE + UREF_SIZE_SERIALIZED;
const LOCAL_SIZE: usize = KEY_ID_SIZE + U32_SIZE + LOCAL_KEY_SIZE;

/// Creates a 32-byte BLAKE2b hash digest from a given a piece of data
fn hash(bytes: &[u8]) -> [u8; LOCAL_KEY_SIZE] {
    let mut ret = [0u8; LOCAL_KEY_SIZE];
    // Safe to unwrap here because our digest length is constant and valid
    let mut hasher = VarBlake2b::new(LOCAL_KEY_SIZE).unwrap();
    hasher.input(bytes);
    hasher.variable_result(|hash| ret.clone_from_slice(hash));
    ret
}

#[repr(C)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Key {
    Account([u8; 32]),
    Hash([u8; HASH_SIZE]),
    URef(URef),
    Local([u8; LOCAL_KEY_SIZE]),
}

impl Key {
    pub fn local(seed: [u8; LOCAL_SEED_SIZE], key_bytes: &[u8]) -> Self {
        let bytes_to_hash: Vec<u8> = seed.iter().chain(key_bytes.iter()).copied().collect();
        let hash: [u8; LOCAL_KEY_SIZE] = hash(&bytes_to_hash);
        Key::Local(hash)
    }

    pub fn type_string(&self) -> String {
        match self {
            Key::Account(_) => String::from("Key::Account"),
            Key::Hash(_) => String::from("Key::Hash"),
            Key::URef(_) => String::from("Key::URef"),
            Key::Local(_) => String::from("Key::Local"),
        }
    }
}

impl core::fmt::Display for Key {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Key::Account(addr) => write!(f, "Key::Account({})", HexFmt(addr)),
            Key::Hash(addr) => write!(f, "Key::Hash({})", HexFmt(addr)),
            Key::URef(uref) => write!(f, "Key::{}", uref), /* Display impl for URef will append */
            // URef(…).
            Key::Local(hash) => write!(f, "Key::Local({})", HexFmt(hash)),
        }
    }
}

impl core::fmt::Debug for Key {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "{}", self)
    }
}

use alloc::string::String;

/// Drops "0x" prefix from the input string and turns rest of it into slice.
fn drop_hex_prefix(s: &str) -> &str {
    if s.starts_with("0x") {
        &s[2..]
    } else {
        &s
    }
}

/// Tries to decode `input` as a 32-byte array.  `input` may be prefixed with "0x".  Returns `None`
/// if `input` cannot be parsed as hex, or if it does not parse to exactly 32 bytes.
fn decode_from_hex(input: &str) -> Option<[u8; HASH_SIZE]> {
    const UNDECORATED_INPUT_LEN: usize = 2 * HASH_SIZE;

    let undecorated_input = drop_hex_prefix(input);

    if undecorated_input.len() != UNDECORATED_INPUT_LEN {
        return None;
    }

    let mut output = [0u8; HASH_SIZE];
    let _bytes_written = base16::decode_slice(undecorated_input, &mut output).ok()?;
    debug_assert!(_bytes_written == HASH_SIZE);
    Some(output)
}

impl Key {
    pub fn to_turef<T>(self) -> Option<TURef<T>> {
        if let Key::URef(uref) = self {
            TURef::from_uref(uref).ok()
        } else {
            None
        }
    }

    pub fn to_c_ptr(self) -> Option<ContractRef> {
        match self {
            Key::URef(uref) => TURef::from_uref(uref).map(ContractRef::TURef).ok(),
            Key::Hash(id) => Some(ContractRef::Hash(id)),
            _ => None,
        }
    }

    /// Returns bytes of an account
    pub fn as_account(&self) -> Option<[u8; 32]> {
        match self {
            Key::Account(bytes) => Some(*bytes),
            _ => None,
        }
    }

    pub fn normalize(self) -> Key {
        match self {
            Key::URef(uref) => Key::URef(uref.remove_access_rights()),
            other => other,
        }
    }

    /// Creates an instance of `Key::Hash` from the hex-encoded string.  Returns `None` if
    /// `hex_encodede_hash` does not decode to a 32-byte array.
    pub fn parse_hash(hex_encodede_hash: &str) -> Option<Key> {
        decode_from_hex(hex_encodede_hash).map(Key::Hash)
    }

    /// Creates an instance of `Key::URef` from the hex-encoded string.  Returns `None` if
    /// `hex_encoded_uref` does not decode to a 32-byte array.
    pub fn parse_uref(hex_encoded_uref: &str, access_rights: AccessRights) -> Option<Key> {
        decode_from_hex(hex_encoded_uref).map(|uref| Key::URef(URef::new(uref, access_rights)))
    }

    /// Creates an instance of `Key::Local` from the hex-encoded strings.  Returns `None` if
    /// `hex_encoded_seed` does not decode to a 32-byte array, or if `hex_encoded_key_bytes` does
    /// does not decode from hex.
    pub fn parse_local(hex_encoded_seed: &str, hex_encoded_key_bytes: &str) -> Option<Key> {
        let decoded_seed = decode_from_hex(hex_encoded_seed)?;
        let decoded_key_bytes = base16::decode(drop_hex_prefix(hex_encoded_key_bytes)).ok()?;
        Some(Key::local(decoded_seed, &decoded_key_bytes))
    }

    pub fn as_string(&self) -> String {
        match self {
            Key::Account(addr) => format!("account-{}", base16::encode_lower(addr)),
            Key::Hash(addr) => format!("hash-{}", base16::encode_lower(addr)),
            Key::URef(uref) => uref.as_string(),
            Key::Local(hash) => format!("local-{}", base16::encode_lower(hash)),
        }
    }

    pub fn as_uref(&self) -> Option<&URef> {
        match self {
            Key::URef(uref) => Some(uref),
            _ => None,
        }
    }

    pub fn as_hash(&self) -> Option<[u8; HASH_SIZE]> {
        match self {
            Key::Hash(hash) => Some(*hash),
            _ => None,
        }
    }

    pub fn as_local(&self) -> Option<[u8; LOCAL_KEY_SIZE]> {
        match self {
            Key::Local(local) => Some(*local),
            _ => None,
        }
    }
}

impl From<URef> for Key {
    fn from(uref: URef) -> Key {
        Key::URef(uref)
    }
}

impl<T> From<TURef<T>> for Key {
    fn from(turef: TURef<T>) -> Self {
        let uref = URef::new(turef.addr(), turef.access_rights());
        Key::URef(uref)
    }
}

impl ToBytes for Key {
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        match self {
            Key::Account(addr) => {
                let mut result = Vec::with_capacity(ACCOUNT_KEY_SIZE);
                result.push(ACCOUNT_ID);
                result.append(&mut addr.to_bytes()?);
                Ok(result)
            }
            Key::Hash(hash) => {
                let mut result = Vec::with_capacity(HASH_KEY_SIZE);
                result.push(HASH_ID);
                result.append(&mut hash.to_bytes()?);
                Ok(result)
            }
            Key::URef(uref) => {
                let mut result = Vec::with_capacity(UREF_SIZE);
                result.push(UREF_ID);
                result.append(&mut uref.to_bytes()?);
                Ok(result)
            }
            Key::Local(hash) => {
                let mut result = Vec::with_capacity(LOCAL_SIZE);
                result.push(LOCAL_ID);
                result.append(&mut hash.to_bytes()?);
                Ok(result)
            }
        }
    }
}

impl FromBytes for Key {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (id, rest): (u8, &[u8]) = FromBytes::from_bytes(bytes)?;
        match id {
            ACCOUNT_ID => {
                let (addr, rem): ([u8; 32], &[u8]) = FromBytes::from_bytes(rest)?;
                Ok((Key::Account(addr), rem))
            }
            HASH_ID => {
                let (hash, rem): ([u8; 32], &[u8]) = FromBytes::from_bytes(rest)?;
                Ok((Key::Hash(hash), rem))
            }
            UREF_ID => {
                let (uref, rem): (URef, &[u8]) = FromBytes::from_bytes(rest)?;
                Ok((Key::URef(uref), rem))
            }
            LOCAL_ID => {
                let (hash, rest): ([u8; 32], &[u8]) = FromBytes::from_bytes(rest)?;
                Ok((Key::Local(hash), rest))
            }
            _ => Err(Error::FormattingError),
        }
    }
}

impl FromBytes for Vec<Key> {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), Error> {
        let (size, rest): (u32, &[u8]) = FromBytes::from_bytes(bytes)?;
        let mut result = Vec::new();
        result.try_reserve_exact(size as usize)?;
        let mut stream = rest;
        for _ in 0..size {
            let (t, rem): (Key, &[u8]) = FromBytes::from_bytes(stream)?;
            result.push(t);
            stream = rem;
        }
        Ok((result, stream))
    }
}

impl ToBytes for Vec<Key> {
    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let size = self.len() as u32;
        let mut result: Vec<u8> = Vec::with_capacity(4 + (size as usize) * UREF_SIZE);
        result.extend(size.to_bytes()?);
        result.extend(
            self.iter()
                .map(ToBytes::to_bytes)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten(),
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    // Can be removed once https://github.com/rust-lang/rustfmt/issues/3362 is resolved.
    #[rustfmt::skip]
    use alloc::vec;
    use alloc::{format, string::String, vec::Vec};

    use proptest::{
        prelude::*,
        string::{string_regex, RegexGeneratorStrategy},
    };

    use crate::{
        bytesrepr::{Error, FromBytes},
        key::{Key, HASH_SIZE, LOCAL_KEY_SIZE},
        uref::{AccessRights, URef},
    };

    fn test_readable(right: AccessRights, is_true: bool) {
        assert_eq!(right.is_readable(), is_true)
    }

    #[test]
    fn test_is_readable() {
        test_readable(AccessRights::READ, true);
        test_readable(AccessRights::READ_ADD, true);
        test_readable(AccessRights::READ_WRITE, true);
        test_readable(AccessRights::READ_ADD_WRITE, true);
        test_readable(AccessRights::ADD, false);
        test_readable(AccessRights::ADD_WRITE, false);
        test_readable(AccessRights::WRITE, false);
    }

    fn test_writable(right: AccessRights, is_true: bool) {
        assert_eq!(right.is_writeable(), is_true)
    }

    #[test]
    fn test_is_writable() {
        test_writable(AccessRights::WRITE, true);
        test_writable(AccessRights::READ_WRITE, true);
        test_writable(AccessRights::ADD_WRITE, true);
        test_writable(AccessRights::READ, false);
        test_writable(AccessRights::ADD, false);
        test_writable(AccessRights::READ_ADD, false);
        test_writable(AccessRights::READ_ADD_WRITE, true);
    }

    fn test_addable(right: AccessRights, is_true: bool) {
        assert_eq!(right.is_addable(), is_true)
    }

    #[test]
    fn test_is_addable() {
        test_addable(AccessRights::ADD, true);
        test_addable(AccessRights::READ_ADD, true);
        test_addable(AccessRights::READ_WRITE, false);
        test_addable(AccessRights::ADD_WRITE, true);
        test_addable(AccessRights::READ, false);
        test_addable(AccessRights::WRITE, false);
        test_addable(AccessRights::READ_ADD_WRITE, true);
    }

    #[test]
    fn should_display_key() {
        let expected_hash = core::iter::repeat("0").take(64).collect::<String>();
        let addr_array = [0u8; 32];
        let account_key = Key::Account(addr_array);
        assert_eq!(
            format!("{}", account_key),
            format!("Key::Account({})", expected_hash)
        );
        let uref_key = Key::URef(URef::new(addr_array, AccessRights::READ));
        assert_eq!(
            format!("{}", uref_key),
            format!("Key::URef({}, READ)", expected_hash)
        );
        let hash_key = Key::Hash(addr_array);
        assert_eq!(
            format!("{}", hash_key),
            format!("Key::Hash({})", expected_hash)
        );
        let local_key = Key::Local(addr_array);
        assert_eq!(
            format!("{}", local_key),
            format!("Key::Local({})", expected_hash)
        );
    }

    #[test]
    fn parse_local_with_arbitrary_length() {
        let short_key = base16::encode_lower(&[42u8; 32]);
        let long_key = base16::encode_lower(&vec![42u8; 255]);
        let seed = "0102010201020102010201020102010201020102010201020102010201020102";

        let local1 = Key::parse_local(seed, &short_key).expect("should parse local with short key");
        let local2 = Key::parse_local(seed, &long_key).expect("should parse local with long key");

        // verifies that the arbitrary key length doesn't get truncated
        assert_ne!(local1, local2);
    }

    /// Create a base16 string of `length` size.
    fn base16_str_arb(length: usize) -> RegexGeneratorStrategy<String> {
        string_regex(&format!("[0-9a-f]{{{}}}", length)).unwrap()
    }

    proptest! {

        #[test]
        fn should_fail_parse_small_base16_to_key(base16_addr in base16_str_arb(32)) {
            assert!(Key::parse_hash(&base16_addr).is_none());
            assert!(Key::parse_uref(&base16_addr, AccessRights::READ).is_none());
            assert!(Key::parse_local(&base16_addr, &base16_addr).is_none());
        }

        #[test]
        fn should_parse_64_base16_to_key(base16_addr in base16_str_arb(64)) {
            assert!(Key::parse_hash(&base16_addr).is_some());
            assert!(Key::parse_uref(&base16_addr, AccessRights::READ).is_some());
            assert!(Key::parse_local(&base16_addr, &base16_addr).is_some());
        }

        #[test]
        fn should_fail_parse_long_base16_to_key(base16_addr in base16_str_arb(70)) {
            assert!(Key::parse_hash(&base16_addr).is_none());
            assert!(Key::parse_uref(&base16_addr, AccessRights::READ).is_none());
            assert!(Key::parse_local(&base16_addr, &base16_addr).is_none());
        }

        #[test]
        fn should_fail_parse_not_base16_input(invalid_addr in "[f-z]{32}") {
            // Only a-f characters are valid hex.
            assert!(Key::parse_hash(&invalid_addr).is_none());
            assert!(Key::parse_uref(&invalid_addr, AccessRights::READ).is_none());
            assert!(Key::parse_local(&invalid_addr, &invalid_addr).is_none());
        }

        #[test]
        fn should_parse_base16_0x_prefixed(base16_addr in base16_str_arb(64)) {
            let preppended = format!("0x{}", &base16_addr);
            assert!(Key::parse_hash(&preppended).is_some());
            assert_eq!(Key::parse_hash(&preppended), Key::parse_hash(&base16_addr));
        }
    }

    #[test]
    fn abuse_vec_key() {
        // Prefix is 2^32-1 = shouldn't allocate that much
        let bytes: Vec<u8> = vec![255, 255, 255, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let res: Result<(Vec<Key>, &[u8]), _> = FromBytes::from_bytes(&bytes);
        #[cfg(target_os = "linux")]
        assert_eq!(res.expect_err("should fail"), Error::OutOfMemoryError);
        #[cfg(target_os = "macos")]
        assert_eq!(res.expect_err("should fail"), Error::EarlyEndOfStream);
    }

    #[test]
    fn decode_from_hex() {
        let input = [255; 32]; // `255` decimal is `ff` hex
        let hex_input = "f".repeat(64);
        assert_eq!(Some(input), super::decode_from_hex(&hex_input));

        let prefixed_hex_input = format!("0x{}", hex_input);
        assert_eq!(Some(input), super::decode_from_hex(&prefixed_hex_input));

        let bad_prefix = format!("0X{}", hex_input);
        assert!(super::decode_from_hex(&bad_prefix).is_none());

        let too_short = "f".repeat(63);
        assert!(super::decode_from_hex(&too_short).is_none());

        let too_long = "f".repeat(65);
        assert!(super::decode_from_hex(&too_long).is_none());

        let invalid_hex = "g".repeat(64);
        assert!(super::decode_from_hex(&invalid_hex).is_none());
    }

    #[test]
    fn check_key_account_getters() {
        let account = [42; 32];
        let key1 = Key::Account(account);
        assert_eq!(key1.as_account(), Some(account));
        assert!(key1.as_hash().is_none());
        assert!(key1.as_uref().is_none());
        assert!(key1.as_local().is_none());
    }

    #[test]
    fn check_key_hash_getters() {
        let hash = [42; HASH_SIZE];
        let key1 = Key::Hash(hash);
        assert!(key1.as_account().is_none());
        assert_eq!(key1.as_hash(), Some(hash));
        assert!(key1.as_uref().is_none());
        assert!(key1.as_local().is_none());
    }

    #[test]
    fn check_key_uref_getters() {
        let uref = URef::new([42; 32], AccessRights::READ_ADD_WRITE);
        let key1 = Key::URef(uref);
        assert!(key1.as_account().is_none());
        assert!(key1.as_hash().is_none());
        assert_eq!(key1.as_uref(), Some(&uref));
        assert!(key1.as_local().is_none());
    }

    #[test]
    fn check_key_local_getters() {
        let local = [42; LOCAL_KEY_SIZE];
        let key1 = Key::Local(local);
        assert!(key1.as_account().is_none());
        assert!(key1.as_hash().is_none());
        assert!(key1.as_uref().is_none());
        assert_eq!(key1.as_local(), Some(local));
    }
}
