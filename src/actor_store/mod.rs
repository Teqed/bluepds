// src/actor_store/mod.rs
pub mod blob;
pub mod db;
pub mod preference;
pub mod record;
pub mod repo;

use anyhow::{Context as _, Result};
use atrium_crypto::keypair::{Export as _, Keypair};
use k256::Secp256k1;
use sqlx::sqlite::SqliteConnectOptions;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::config::AppConfig;
use blob::{BlobReader, BlobTransactor};
use db::ActorDb;
use preference::{PreferenceReader, PreferenceTransactor};
use record::{RecordReader, RecordTransactor};
use repo::{RepoReader, RepoTransactor};

pub struct ActorStore {
    pub config: Arc<AppConfig>,
}

impl ActorStore {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { config }
    }

    pub async fn exists(&self, did: &str) -> Result<bool> {
        let path = self.get_db_path(did)?;
        Ok(tokio::fs::try_exists(&path).await.unwrap_or(false))
    }

    pub async fn keypair(&self, did: &str) -> Result<Keypair<Secp256k1>> {
        let path = self.get_keypair_path(did)?;
        let key_data = tokio::fs::read(&path)
            .await
            .context("failed to read keypair")?;
        Ok(Keypair::<Secp256k1>::import(&key_data)?)
    }

    async fn get_db(&self, did: &str) -> Result<ActorDb> {
        let path = self.get_db_path(did)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", path.display()))?
            .create_if_missing(true);
        let pool = sqlx::SqlitePool::connect_with(options).await?;

        Ok(ActorDb::new(pool))
    }

    pub async fn read<F, T>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreReader) -> Result<T>,
    {
        let db = self.get_db(did).await?;
        let keypair = Arc::new(self.keypair(did).await?);
        let reader = ActorStoreReader::new(did.to_string(), db, self.config.clone(), keypair);
        f(reader)
    }

    pub async fn transact<F, T>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = self.get_db(did).await?;
        let keypair = Arc::new(self.keypair(did).await?);

        db.transaction(|_| {
            let transactor = ActorStoreTransactor::new(
                did.to_string(),
                db.clone(),
                self.config.clone(),
                keypair.clone(),
            );
            f(transactor)
        })
        .await
    }

    pub async fn create(&self, did: &str, keypair: &Keypair<Secp256k1>) -> Result<()> {
        // Create directory structure
        let db_path = self.get_db_path(did)?;
        if let Some(parent) = db_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Export keypair
        let key_path = self.get_keypair_path(did)?;
        let key_data = keypair.export();
        tokio::fs::write(key_path, key_data).await?;

        // Create and initialize DB
        let db = self.get_db(did).await?;
        db.migrate().await?;

        Ok(())
    }

    fn get_db_path(&self, did: &str) -> Result<PathBuf> {
        let did_hash = did
            .strip_prefix("did:plc:")
            .context("DID must be a PLC identifier")?;
        Ok(self.config.repo.path.join(format!("{}.db", did_hash)))
    }

    fn get_keypair_path(&self, did: &str) -> Result<PathBuf> {
        let did_hash = did
            .strip_prefix("did:plc:")
            .context("DID must be a PLC identifier")?;
        Ok(self.config.repo.path.join(format!("{}.key", did_hash)))
    }
}

pub struct ActorStoreReader {
    pub did: String,
    db: ActorDb,
    config: Arc<AppConfig>,
    keypair: Arc<Keypair<Secp256k1>>,
}

impl ActorStoreReader {
    fn new(
        did: String,
        db: ActorDb,
        config: Arc<AppConfig>,
        keypair: Arc<Keypair<Secp256k1>>,
    ) -> Self {
        Self {
            did,
            db,
            config,
            keypair,
        }
    }

    pub fn blob(&self) -> BlobReader {
        BlobReader::new(self.db.clone())
    }

    pub fn record(&self) -> RecordReader {
        RecordReader::new(self.db.clone())
    }

    pub fn repo(&self) -> RepoReader {
        RepoReader::new(self.db.clone(), self.did.clone())
    }

    pub fn pref(&self) -> PreferenceReader {
        PreferenceReader::new(self.db.clone())
    }
}

pub struct ActorStoreTransactor {
    pub did: String,
    db: ActorDb,
    config: Arc<AppConfig>,
    keypair: Arc<Keypair<Secp256k1>>,
}

impl ActorStoreTransactor {
    fn new(
        did: String,
        db: ActorDb,
        config: Arc<AppConfig>,
        keypair: Arc<Keypair<Secp256k1>>,
    ) -> Self {
        Self {
            did,
            db,
            config,
            keypair,
        }
    }

    pub fn blob(&self) -> BlobTransactor {
        BlobTransactor::new(self.db.clone())
    }

    pub fn record(&self) -> RecordTransactor {
        RecordTransactor::new(self.db.clone())
    }

    pub fn repo(&self) -> RepoTransactor {
        RepoTransactor::new(self.db.clone(), self.keypair.clone(), self.did.clone())
    }

    pub fn pref(&self) -> PreferenceTransactor {
        PreferenceTransactor::new(self.db.clone())
    }
}
