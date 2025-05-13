use anyhow::Result;
use diesel::prelude::*;
use std::sync::Arc;

use super::{
    ActorStoreTransactor, actor_store_resources::ActorStoreResources, db::ActorDb,
    preference::PreferenceReader, record::RecordReader, repo::RepoReader,
};
use crate::SigningKey;

/// Reader for the actor store.
pub(crate) struct ActorStoreReader {
    /// DID of the actor.
    pub(crate) did: String,
    /// Record reader.
    pub(crate) record: RecordReader,
    /// Preference reader.
    pub(crate) pref: PreferenceReader,
    /// RepoReader
    pub(crate) repo: RepoReader,
    /// Function to get keypair.
    keypair_fn: Box<dyn Fn() -> Result<Arc<SigningKey>> + Send + Sync>,
    /// Database connection.
    pub(crate) db: ActorDb,
    /// Actor store resources.
    resources: ActorStoreResources,
}

impl ActorStoreReader {
    /// Create a new actor store reader.
    pub(crate) fn new(
        did: String,
        db: ActorDb,
        resources: ActorStoreResources,
        keypair: impl Fn() -> Result<Arc<SigningKey>> + Send + Sync + 'static,
    ) -> Self {
        // Create readers
        let record = RecordReader::new(db.clone(), did.clone());
        let pref = PreferenceReader::new(db.clone(), did.clone());

        // Store keypair function for later use
        let keypair_fn = Box::new(keypair);

        // Initial keypair call as in TypeScript implementation
        let _ = keypair_fn();

        // Create repo reader
        let repo = RepoReader::new(db.clone(), resources.blobstore(did.clone()), did.clone());

        Self {
            did,
            repo,
            record,
            pref,
            keypair_fn,
            db,
            resources,
        }
    }

    /// Get the keypair for this actor.
    pub(crate) fn keypair(&self) -> Result<Arc<SigningKey>> {
        (self.keypair_fn)()
    }

    /// Execute a transaction with the actor store.
    pub(crate) async fn transact<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T> + Send,
        T: Send + 'static,
    {
        let keypair = self.keypair()?;
        let did = self.did.clone();
        let resources = self.resources.clone();

        self.db
            .transaction(move |conn| {
                // Create a transactor with the transaction
                // We'll create a temporary ActorDb with the same pool
                let db = ActorDb {
                    pool: self.db.pool.clone(),
                };

                let store = ActorStoreTransactor::new(did, db, keypair.clone(), resources);

                // Execute user function
                f(store)
            })
            .await
    }
}
