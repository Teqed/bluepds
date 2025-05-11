//! ReadableBlockstore for managing blocks in the repository.

use crate::repo::block_map::{BlockMap, MissingBlockError};
use crate::repo::util::{cbor_to_lex_record, parse_obj_by_def};
use anyhow::{Context, Result};
use atrium_repo::Cid;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for a readable blockstore.
pub(crate) trait ReadableBlockstore {
    /// Get the raw bytes for a given CID.
    fn get_bytes(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;

    /// Check if a block exists for a given CID.
    fn has(&self, cid: &Cid) -> Result<bool>;

    /// Get multiple blocks for a list of CIDs.
    fn get_blocks(&self, cids: &[Cid]) -> Result<(BlockMap, Vec<Cid>)>;

    /// Attempt to read an object by CID and definition.
    fn attempt_read<T>(&self, cid: &Cid, def: &str) -> Result<Option<(T, Vec<u8>)>>
    where
        T: serde::de::DeserializeOwned;

    /// Read an object and its raw bytes by CID and definition.
    fn read_obj_and_bytes<T>(&self, cid: &Cid, def: &str) -> Result<(T, Vec<u8>)>
    where
        T: serde::de::DeserializeOwned;

    /// Read an object by CID and definition.
    fn read_obj<T>(&self, cid: &Cid, def: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned;

    /// Attempt to read a record by CID.
    fn attempt_read_record(&self, cid: &Cid) -> Result<Option<RepoRecord>>;

    /// Read a record by CID.
    fn read_record(&self, cid: &Cid) -> Result<RepoRecord>;
}

/// Concrete implementation of the ReadableBlockstore.
pub(crate) struct InMemoryBlockstore {
    blocks: HashMap<Cid, Vec<u8>>,
}

impl InMemoryBlockstore {
    /// Create a new in-memory blockstore.
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
        }
    }

    /// Add a block to the blockstore.
    pub fn add_block(&mut self, cid: Cid, bytes: Vec<u8>) {
        self.blocks.insert(cid, bytes);
    }
}

impl ReadableBlockstore for InMemoryBlockstore {
    fn get_bytes(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.get(cid).cloned())
    }

    fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(self.blocks.contains_key(cid))
    }

    fn get_blocks(&self, cids: &[Cid]) -> Result<(BlockMap, Vec<Cid>)> {
        let mut blocks = BlockMap::new();
        let mut missing = Vec::new();

        for cid in cids {
            if let Some(bytes) = self.blocks.get(cid) {
                blocks.insert(*cid, bytes.clone());
            } else {
                missing.push(*cid);
            }
        }

        Ok((blocks, missing))
    }

    fn attempt_read<T>(&self, cid: &Cid, def: &str) -> Result<Option<(T, Vec<u8>)>>
    where
        T: serde::de::DeserializeOwned,
    {
        if let Some(bytes) = self.get_bytes(cid)? {
            let obj = parse_obj_by_def(&bytes, cid, def)?;
            Ok(Some((obj, bytes)))
        } else {
            Ok(None)
        }
    }

    fn read_obj_and_bytes<T>(&self, cid: &Cid, def: &str) -> Result<(T, Vec<u8>)>
    where
        T: serde::de::DeserializeOwned,
    {
        if let Some((obj, bytes)) = self.attempt_read(cid, def)? {
            Ok((obj, bytes))
        } else {
            Err(MissingBlockError::new(*cid, def).into())
        }
    }

    fn read_obj<T>(&self, cid: &Cid, def: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let (obj, _) = self.read_obj_and_bytes(cid, def)?;
        Ok(obj)
    }

    fn attempt_read_record(&self, cid: &Cid) -> Result<Option<RepoRecord>> {
        match self.read_record(cid) {
            Ok(record) => Ok(Some(record)),
            Err(_) => Ok(None),
        }
    }

    fn read_record(&self, cid: &Cid) -> Result<RepoRecord> {
        if let Some(bytes) = self.get_bytes(cid)? {
            cbor_to_lex_record(&bytes)
        } else {
            Err(MissingBlockError::new(*cid, "RepoRecord").into())
        }
    }
}
