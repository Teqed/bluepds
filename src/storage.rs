//! ATProto user repository datastore functionality.

use anyhow::{Context, Result};
use atrium_repo::{
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore},
    Cid, Repository,
};

use crate::config::RepoConfig;

pub async fn open_store(
    config: &RepoConfig,
    did: impl AsRef<str>,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    let did = did.as_ref();
    let id = did
        .strip_prefix("did:plc:")
        .context("did in unknown format")?;

    let p = config.path.join(id).with_extension("car");

    let f = tokio::fs::File::open(p)
        .await
        .context("failed to open repository file")?;

    Ok(CarStore::open(f)
        .await
        .context("failed to open car store")?)
}

pub async fn open_repo(
    config: &RepoConfig,
    did: impl AsRef<str>,
    cid: Cid,
) -> Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let store = open_store(config, did)
        .await
        .context("failed to open storage")?;

    Ok(Repository::open(store, cid)
        .await
        .context("failed to open repo")?)
}
