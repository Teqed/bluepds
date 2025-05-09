//! Repository reader for the actor store.

use anyhow::Result;
use atrium_repo::Cid;
use rsky_repo::block_map::BlockMap;
use sqlx::SqlitePool;

use super::sql_repo_reader::SqlRepoReader;
use crate::config::BlobConfig;

/// Reader for repository data in the actor store.
pub(crate) struct RepoReader {
    /// The SQL repository reader.
    pub storage: SqlRepoReader,
    /// The database connection.
    pub db: SqlitePool,
    /// The DID of the repository owner.
    pub did: String,
}

impl RepoReader {
    /// Create a new repository reader.
    pub(crate) fn new(db: SqlitePool, did: String, blob_config: BlobConfig) -> Self {
        Self {
            storage: SqlRepoReader::new(db.clone(), did.clone()),
            db,
            did,
        }
    }

    /// Get event data for synchronization.
    pub(crate) async fn get_sync_event_data(&self) -> Result<SyncEventData> {
        let root = self.storage.get_root_detailed().await?;
        let blocks = self.storage.get_blocks(vec![root.cid]).await?;

        Ok(SyncEventData {
            cid: root.cid,
            rev: root.rev,
            blocks: blocks.blocks,
        })
    }
}

/// Data for sync events.
pub(crate) struct SyncEventData {
    /// The CID of the repository root.
    pub cid: Cid,
    /// The revision of the repository.
    pub rev: String,
    /// The blocks in the repository.
    pub blocks: BlockMap,
}
