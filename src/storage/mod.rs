//! `ATProto` user repository datastore functionality.

pub(crate) mod car;
mod sqlite;

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid, Repository,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore, Error as BlockstoreError},
};
use std::str::FromStr as _;

use crate::{Db, config::RepoConfig};

// Re-export public items
pub(crate) use car::open_car_store;
pub(crate) use sqlite::{SQLiteStore, open_sqlite_store};

/// Open a repository for a given DID.
pub(crate) async fn open_repo_db(
    config: &RepoConfig,
    db: &Db,
    did: impl Into<String>,
) -> Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let did = did.into();
    let cid = sqlx::query_scalar!(
        r#"
        SELECT root FROM accounts
        WHERE did = ?
        "#,
        did
    )
    .fetch_one(db)
    .await
    .context("failed to query database")?;

    open_repo(
        config,
        did,
        Cid::from_str(&cid).context("should be valid CID")?,
    )
    .await
}

/// Open a repository for a given DID and CID.
pub(crate) async fn open_repo(
    config: &RepoConfig,
    did: impl Into<String>,
    cid: Cid,
) -> Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let store = open_car_store(config, did.into()).await?;
    Repository::open(store, cid)
        .await
        .context("failed to open repo")
}
/// Open a repository for a given DID and CID.
/// SQLite backend.
pub(crate) async fn open_repo_sqlite(
    config: &RepoConfig,
    did: impl Into<String>,
    cid: Cid,
) -> Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let store = open_sqlite_store(config, did.into()).await?;
    return Repository::open(store, cid)
        .await
        .context("failed to open repo");
}

/// Open a block store for a given DID.
pub(crate) async fn open_store(
    config: &RepoConfig,
    did: impl Into<String>,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    let did = did.into();

    // if config.use_sqlite {
    return open_sqlite_store(config, did.clone()).await;
    // }
    // Default to CAR store
    // open_car_store(config, &did).await
}

/// Create a storage backend for a DID
pub(crate) async fn create_storage_for_did(
    config: &RepoConfig,
    did_hash: &str,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    // Use standard file structure but change extension based on type
    // if config.use_sqlite {
    // For SQLite, create a new database file
    let db_path = config.path.join(format!("{}.db", did_hash));

    // Ensure parent directory exists
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("failed to create directory")?;
    }

    // Create SQLite store
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true),
        )
        .await
        .context("failed to connect to SQLite database")?;

    // Initialize tables
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
            PRAGMA journal_mode=WAL;
        ",
    )
    .execute(&pool)
    .await
    .context("failed to create tables")?;

    Ok(SQLiteStore {
        pool,
        did: format!("did:plc:{}", did_hash),
    })
    // } else {
    //     // For CAR files, create a new file
    //     let file_path = config.path.join(format!("{}.car", did_hash));

    //     // Ensure parent directory exists
    //     if let Some(parent) = file_path.parent() {
    //         tokio::fs::create_dir_all(parent)
    //             .await
    //             .context("failed to create directory")?;
    //     }

    //     let file = tokio::fs::File::create_new(file_path)
    //         .await
    //         .context("failed to create repo file")?;

    //     CarStore::create(file)
    //         .await
    //         .context("failed to create carstore")
    // }
}
