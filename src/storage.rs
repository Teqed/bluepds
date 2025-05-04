//! ATProto user repository datastore functionality.

use std::str::FromStr;

use anyhow::{Context, Result};
use atrium_repo::{
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore},
    Cid, Repository,
};

use crate::{config::RepoConfig, mmap::MappedFile, Db};

pub async fn open_store(
    config: &RepoConfig,
    did: impl Into<String>,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    let did = did.into();
    let id = did
        .strip_prefix("did:plc:")
        .context("did in unknown format")?;

    let p = config.path.join(id).with_extension("car");

    let f = std::fs::File::options()
        .read(true)
        .write(true)
        .open(p)
        .context("failed to open repository file")?;
    let f = MappedFile::new(f).context("failed to map repo")?;

    Ok(CarStore::open(f)
        .await
        .context("failed to open car store")?)
}

pub async fn open_repo_db(
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

    open_repo(config, did, Cid::from_str(&cid).unwrap()).await
}

pub async fn open_repo(
    config: &RepoConfig,
    did: impl Into<String>,
    cid: Cid,
) -> Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let did = did.into();
    let store = open_store(config, did)
        .await
        .context("failed to open storage")?;

    Ok(Repository::open(store, cid)
        .await
        .context("failed to open repo")?)
}
