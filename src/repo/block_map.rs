//! BlockMap impl
//! Ref: https://github.com/blacksky-algorithms/rsky/blob/main/rsky-repo/src/block_map.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//! Reimplemented here because of conflicting dependencies between atrium and rsky

use anyhow::Result;
use atrium_repo::Cid;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;

/// Ref: https://github.com/blacksky-algorithms/rsky/blob/main/rsky-repo/src/cid_set.rs
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct CidSet {
    pub set: HashSet<String>,
}
impl CidSet {
    pub(crate) fn new(arr: Option<Vec<Cid>>) -> Self {
        let str_arr: Vec<String> = arr
            .unwrap_or(Vec::new())
            .into_iter()
            .map(|cid| cid.to_string())
            .collect::<Vec<String>>();
        CidSet {
            set: HashSet::from_iter(str_arr),
        }
    }

    pub(crate) fn add(&mut self, cid: Cid) -> () {
        let _ = &self.set.insert(cid.to_string());
        ()
    }

    pub(crate) fn add_set(&mut self, to_merge: CidSet) -> () {
        for cid in to_merge.to_list() {
            let _ = &self.add(cid);
        }
        ()
    }

    pub(crate) fn subtract_set(&mut self, to_subtract: CidSet) -> () {
        for cid in to_subtract.to_list() {
            self.delete(cid);
        }
        ()
    }

    pub(crate) fn delete(&mut self, cid: Cid) -> () {
        self.set.remove(&cid.to_string());
        ()
    }

    pub(crate) fn has(&self, cid: Cid) -> bool {
        self.set.contains(&cid.to_string())
    }

    pub(crate) fn size(&self) -> usize {
        self.set.len()
    }

    pub(crate) fn clear(mut self) -> () {
        self.set.clear();
        ()
    }

    pub(crate) fn to_list(&self) -> Vec<Cid> {
        self.set
            .clone()
            .into_iter()
            .filter_map(|cid| match Cid::from_str(&cid) {
                Ok(r) => Some(r),
                Err(_) => None,
            })
            .collect::<Vec<Cid>>()
    }
}

/// Ref: https://github.com/blacksky-algorithms/rsky/blob/main/rsky-common/src/lib.rs#L57
pub(crate) fn struct_to_cbor<T: Serialize>(obj: &T) -> Result<Vec<u8>> {
    Ok(serde_ipld_dagcbor::to_vec(obj)?)
}

/// Ref: https://github.com/blacksky-algorithms/rsky/blob/37954845d06aaafea2b914d9096a1657abfc8d75/rsky-common/src/ipld.rs
/// Create a CID for CBOR-encoded data
pub(crate) fn cid_for_cbor<T: Serialize>(data: &T) -> Result<Cid> {
    let bytes = struct_to_cbor(data)?;
    let multihash = atrium_repo::Multihash::wrap(
        atrium_repo::blockstore::SHA2_256,
        &sha2::Sha256::digest(&bytes),
    )
    .expect("valid multihash");

    // Use the constant for DAG_CBOR (0x71) directly instead of converting from DagCborCodec
    Ok(Cid::new_v1(0x71, multihash))
}

/// Create a CID from a SHA-256 hash with the specified codec
pub(crate) fn sha256_to_cid(hash: Vec<u8>, codec: u64) -> Cid {
    let multihash = atrium_repo::Multihash::wrap(atrium_repo::blockstore::SHA2_256, &hash)
        .expect("valid multihash");

    Cid::new_v1(codec, multihash)
}

/// Create a CID from a raw SHA-256 hash (using raw codec 0x55)
pub(crate) fn sha256_raw_to_cid(hash: Vec<u8>) -> Cid {
    sha256_to_cid(hash, 0x55) // 0x55 is the codec for raw
}

/// Ref: https://github.com/blacksky-algorithms/rsky/blob/main/rsky-repo/src/types.rs#L436
pub(crate) type CarBlock = CidAndBytes;
pub(crate) struct CidAndBytes {
    pub cid: Cid,
    pub bytes: Vec<u8>,
}

// Thinly wraps a Vec<u8>
// The #[serde(transparent)] attribute ensures that during (de)serialization
// this newtype is treated the same as the underlying Vec<u8>.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct Bytes(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct BlockMap {
    pub map: BTreeMap<String, Bytes>,
}

impl BlockMap {
    pub(crate) fn new() -> Self {
        BlockMap {
            map: BTreeMap::new(),
        }
    }

    pub(crate) fn add<T: Serialize>(&mut self, value: T) -> Result<Cid> {
        let cid = cid_for_cbor(&value)?;
        self.set(
            cid,
            struct_to_cbor(&value)?, //bytes
        );
        Ok(cid)
    }

    pub(crate) fn set(&mut self, cid: Cid, bytes: Vec<u8>) -> () {
        self.map.insert(cid.to_string(), Bytes(bytes));
        ()
    }

    pub(crate) fn get(&self, cid: Cid) -> Option<&Vec<u8>> {
        self.map.get(&cid.to_string()).map(|bytes| &bytes.0)
    }
    pub(crate) fn delete(&mut self, cid: Cid) -> Result<()> {
        self.map.remove(&cid.to_string());
        Ok(())
    }

    pub(crate) fn get_many(&mut self, cids: Vec<Cid>) -> Result<BlocksAndMissing> {
        let mut missing: Vec<Cid> = Vec::new();
        let mut blocks = BlockMap::new();
        for cid in cids {
            let got = self.map.get(&cid.to_string()).map(|bytes| &bytes.0);
            if let Some(bytes) = got {
                blocks.set(cid, bytes.clone());
            } else {
                missing.push(cid);
            }
        }
        Ok(BlocksAndMissing { blocks, missing })
    }

    pub(crate) fn has(&self, cid: Cid) -> bool {
        self.map.contains_key(&cid.to_string())
    }

    pub(crate) fn clear(&mut self) -> () {
        self.map.clear()
    }

    // Not really using. Issues with closures
    pub(crate) fn for_each(&self, cb: impl Fn(&Vec<u8>, Cid) -> ()) -> Result<()> {
        for (key, val) in self.map.iter() {
            cb(&val.0, Cid::from_str(&key)?);
        }
        Ok(())
    }

    pub(crate) fn entries(&self) -> Result<Vec<CidAndBytes>> {
        let mut entries: Vec<CidAndBytes> = Vec::new();
        for (cid, bytes) in self.map.iter() {
            entries.push(CidAndBytes {
                cid: Cid::from_str(cid)?,
                bytes: bytes.0.clone(),
            });
        }
        Ok(entries)
    }

    pub(crate) fn cids(&self) -> Result<Vec<Cid>> {
        Ok(self.entries()?.into_iter().map(|e| e.cid).collect())
    }

    pub(crate) fn add_map(&mut self, to_add: BlockMap) -> Result<()> {
        let results = for (cid, bytes) in to_add.map.iter() {
            self.set(Cid::from_str(cid)?, bytes.0.clone());
        };
        Ok(results)
    }

    pub(crate) fn size(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn byte_size(&self) -> Result<usize> {
        let mut size = 0;
        for (_, bytes) in self.map.iter() {
            size += bytes.0.len();
        }
        Ok(size)
    }

    pub(crate) fn equals(&self, other: BlockMap) -> Result<bool> {
        if self.size() != other.size() {
            return Ok(false);
        }
        for entry in self.entries()? {
            let other_bytes = other.get(entry.cid);
            if let Some(o) = other_bytes {
                if &entry.bytes != o {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

// Helper function for the iterator conversion.
fn to_cid_and_bytes((cid_str, bytes): (String, Bytes)) -> CidAndBytes {
    // We assume the key is always valid; otherwise, you could handle the error.
    let cid = Cid::from_str(&cid_str).expect("BlockMap contains an invalid CID string");
    CidAndBytes {
        cid,
        bytes: bytes.0,
    }
}

impl IntoIterator for BlockMap {
    type Item = CidAndBytes;
    // Using the iterator returned by BTreeMap's into_iter, then mapping with a function pointer.
    type IntoIter = std::iter::Map<
        std::collections::btree_map::IntoIter<String, Bytes>,
        fn((String, Bytes)) -> CidAndBytes,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter().map(to_cid_and_bytes)
    }
}

#[derive(Debug)]
pub(crate) struct BlocksAndMissing {
    pub blocks: BlockMap,
    pub missing: Vec<Cid>,
}
