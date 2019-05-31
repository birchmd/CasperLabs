use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::ops::Add;

pub fn add<K, V, H: BuildHasher>(map: &mut HashMap<K, V, H>, k: K, v: V)
where
    K: Eq + Hash,
    V: Add<Output = V>,
{
    let maybe_curr = map.remove(&k);
    match maybe_curr {
        None => {
            let _ = map.insert(k, v);
        }
        Some(curr) => {
            let _ = map.insert(k, curr + v);
        }
    }
}
