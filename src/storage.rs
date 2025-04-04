//! `ATProto` user repository datastore functionality.

use std::str::FromStr as _;

use anyhow::{Context as _, Result};
use atrium_repo::{
    Cid, Repository,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore},
};

use crate::{Db, config::RepoConfig};

/// Open a block store for a given DID.
pub(crate) async fn open_store(
    config: &RepoConfig,
    did: impl Into<String>,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    let did = did.into();
    let id = did
        .strip_prefix("did:plc:")
        .context("did in unknown format")?;

    let p = config.path.join(id).with_extension("car");

    let f = tokio::fs::File::options()
        .read(true)
        .write(true)
        .open(p)
        .await
        .context("failed to open repository file")?;

    CarStore::open(f).await.context("failed to open car store")
}

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
    let did = did.into();
    let store = open_store(config, did)
        .await
        .context("failed to open storage")?;

    Repository::open(store, cid)
        .await
        .context("failed to open repo")
}
