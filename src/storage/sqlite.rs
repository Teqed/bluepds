//! SQLite-based repository storage implementation.

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid, Multihash,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, Error as BlockstoreError},
};
use sha2::Digest;
use sqlx::SqlitePool;

use crate::config::RepoConfig;

/// SQLite-based implementation of block storage.
pub(crate) struct SQLiteStore {
    pub did: String,
    pub pool: SqlitePool,
}

impl AsyncBlockStoreRead for SQLiteStore {
    async fn read_block(&mut self, cid: Cid) -> Result<Vec<u8>, BlockstoreError> {
        tracing::info!("Reading block with CID: {}", cid);
        let mut contents = Vec::new();
        self.read_block_into(cid, &mut contents).await?;
        Ok(contents)
    }
    fn read_block_into(
        &mut self,
        cid: Cid,
        contents: &mut Vec<u8>,
    ) -> impl Future<Output = Result<(), BlockstoreError>> + Send {
        tracing::info!("Reading block into buffer with CID: {}", cid);
        let cid_str = cid.to_string();
        let pool = self.pool.clone();

        tracing::info!("Async moving block read");
        async move {
            tracing::info!("Async move block read");
            // let record = sqlx::query!(r#"SELECT data FROM blocks WHERE cid = ?"#, cid_str)
            //     .fetch_optional(&pool)
            //     .await
            //     .map_err(|e| BlockstoreError::Other(Box::new(e)))?
            //     .ok_or(BlockstoreError::CidNotFound)?;
            let record = sqlx::query!(r#"SELECT data FROM blocks WHERE cid = ?"#, cid_str)
                .fetch_optional(&pool)
                .await;
            tracing::info!("Record fetched: {:?}", record);
            let record = match record {
                Ok(Some(record)) => record,
                Ok(None) => return Err(BlockstoreError::CidNotFound),
                Err(e) => return Err(BlockstoreError::Other(Box::new(e))),
            };
            tracing::info!("Block read successful");

            contents.clear();
            tracing::info!("Contents cleared");
            contents.extend_from_slice(&record.data);
            tracing::info!("Contents extended");
            Ok(())
        }
    }
}

impl AsyncBlockStoreWrite for SQLiteStore {
    fn write_block(
        &mut self,
        codec: u64,
        hash: u64,
        contents: &[u8],
    ) -> impl Future<Output = Result<Cid, BlockstoreError>> + Send {
        let contents = contents.to_vec(); // Clone the data
        let pool = self.pool.clone();

        async move {
            let digest = match hash {
                atrium_repo::blockstore::SHA2_256 => sha2::Sha256::digest(&contents),
                _ => return Err(BlockstoreError::UnsupportedHash(hash)),
            };

            let multihash = Multihash::wrap(hash, digest.as_slice())
                .map_err(|_| BlockstoreError::UnsupportedHash(hash))?;

            let cid = Cid::new_v1(codec, multihash);
            let cid_str = cid.to_string();

            // Use a transaction for atomicity
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| BlockstoreError::Other(Box::new(e)))?;

            // Check if block already exists
            let exists =
                sqlx::query_scalar!(r#"SELECT COUNT(*) FROM blocks WHERE cid = ?"#, cid_str)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| BlockstoreError::Other(Box::new(e)))?;

            // Only insert if block doesn't exist
            let codec = codec as i64;
            let hash = hash as i64;
            if exists == 0 {
                _ = sqlx::query!(
                    r#"INSERT INTO blocks (cid, data, multicodec, multihash) VALUES (?, ?, ?, ?)"#,
                    cid_str,
                    contents,
                    codec,
                    hash
                )
                .execute(&mut *tx)
                .await
                .map_err(|e| BlockstoreError::Other(Box::new(e)))?;
            }

            tx.commit()
                .await
                .map_err(|e| BlockstoreError::Other(Box::new(e)))?;

            Ok(cid)
        }
    }
}

/// Open a SQLite store for the given DID.
pub(crate) async fn open_sqlite_store(
    config: &RepoConfig,
    did: impl Into<String>,
) -> Result<SQLiteStore> {
    tracing::info!("Opening SQLite store for DID");
    let did_str = did.into();

    // Extract the PLC ID from the DID
    let id = did_str
        .strip_prefix("did:plc:")
        .context("DID in unknown format")?;

    // Create database connection pool
    let db_path = config.path.join(format!("{id}.db"));

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true),
        )
        .await
        .context("failed to connect to SQLite database")?;

    // Ensure tables exist
    _ = sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS blocks (
            cid TEXT PRIMARY KEY NOT NULL,
            data BLOB NOT NULL,
            multicodec INTEGER NOT NULL,
            multihash INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tree_nodes (
            repo_did TEXT NOT NULL,
            key TEXT NOT NULL,
            value_cid TEXT NOT NULL,
            PRIMARY KEY (repo_did, key),
            FOREIGN KEY (value_cid) REFERENCES blocks(cid)
        );
        CREATE INDEX IF NOT EXISTS idx_blocks_cid ON blocks(cid);
        CREATE INDEX IF NOT EXISTS idx_tree_nodes_repo ON tree_nodes(repo_did);
        ",
    )
    .execute(&pool)
    .await
    .context("failed to create tables")?;

    Ok(SQLiteStore { pool, did: did_str })
}
