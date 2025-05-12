use anyhow::Result;
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
    db: ActorDb,
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
        let record = RecordReader::new(db.clone());
        let pref = PreferenceReader::new(db.clone());

        // Store keypair function for later use
        let keypair_fn = Box::new(keypair);

        // Initial keypair call as in TypeScript implementation
        let _ = keypair_fn();

        // Create repo reader
        let repo = RepoReader::new(db.clone(), resources.blobstore(did.clone()));

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
        F: FnOnce(ActorStoreTransactor) -> Result<T>,
    {
        let keypair = self.keypair()?;
        let did = self.did.clone();
        let resources = self.resources.clone();

        self.db
            .transaction_no_retry(move |tx| {
                // Create a transactor with the transaction
                let store = ActorStoreTransactor::new_with_transaction(
                    did,
                    tx, // Pass the transaction directly
                    keypair.clone(),
                    resources,
                );

                // Execute user function
                f(store).map_err(|e| sqlx::Error::Custom(Box::new(e))) // Convert anyhow::Error to sqlx::Error
            })
            .await
            .map_err(|e| anyhow::anyhow!("Transaction error: {:?}", e))
    }
}
