use std::sync::Arc;

use super::{
    ActorDb, actor_store_resources::ActorStoreResources, preference::PreferenceTransactor,
    record::RecordTransactor, repo::RepoTransactor,
};
use crate::SigningKey;

/// Actor store transactor for managing actor-related transactions.
pub(crate) struct ActorStoreTransactor {
    /// Record transactor.
    pub(crate) record: RecordTransactor,
    /// Repo transactor.
    pub(crate) repo: RepoTransactor,
    /// Preference transactor.
    pub(crate) pref: PreferenceTransactor,
}
impl ActorStoreTransactor {
    /// Creates a new actor store transactor.
    ///
    /// # Arguments
    ///
    /// * `did` - The DID of the actor.
    /// * `db` - The database connection.
    /// * `keypair` - The signing keypair.
    /// * `resources` - The actor store resources.
    pub(crate) fn new(
        did: String,
        db: ActorDb,
        keypair: Arc<SigningKey>,
        resources: ActorStoreResources,
    ) -> Self {
        let blobstore = resources.blobstore(did.clone());

        let record = RecordTransactor::new(db.clone(), blobstore.clone());
        let pref = PreferenceTransactor::new(db.clone());
        let repo = RepoTransactor::new(
            db,
            blobstore,
            did,
            keypair,
            resources.background_queue,
            None,
        );

        Self { record, repo, pref }
    }
}
