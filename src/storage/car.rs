//! CAR file-based repository storage

use anyhow::{Context as _, Result};
use atrium_repo::blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore};

use crate::{config::RepoConfig, mmap::MappedFile};

/// Open a CAR block store for a given DID.
pub(crate) async fn open_car_store(
    config: &RepoConfig,
    did: impl AsRef<str>,
) -> Result<impl AsyncBlockStoreRead + AsyncBlockStoreWrite> {
    let id = did
        .as_ref()
        .strip_prefix("did:plc:")
        .context("did in unknown format")?;

    let p = config.path.join(id).with_extension("car");

    let f = std::fs::File::options()
        .read(true)
        .write(true)
        .open(p)
        .context("failed to open repository file")?;
    let f = MappedFile::new(f).context("failed to map repo")?;

    CarStore::open(f).await.context("failed to open car store")
}
