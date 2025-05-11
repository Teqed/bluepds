use std::sync::Arc;

use super::{
    ActorStoreTransactor, db::ActorDb, preference::PreferenceReader, record::RecordReader,
    repo::RepoReader, resources::ActorStoreResources,
};
use crate::SigningKey;

pub(crate) struct ActorStoreReader {
    pub(crate) repo: RepoReader,
    pub(crate) record: RecordReader,
    pub(crate) pref: PreferenceReader,
}

impl ActorStoreReader {
    pub(crate) fn new(
        did: String,
        db: ActorDb,
        resources: ActorStoreResources,
        keypair: impl Fn() -> Result<Arc<SigningKey>>,
    ) -> Self {
        let blobstore = resources.blobstore(&did);

        let repo = RepoReader::new(db.clone(), blobstore);
        let record = RecordReader::new(db.clone());
        let pref = PreferenceReader::new(db);
        keypair();

        Self { repo, record, pref }
    }

    pub(crate) async fn transact<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T>,
    {
        let keypair = self.keypair();
        let db_txn = self.db.transaction().await?;
        let store =
            ActorStoreTransactor::new(self.did.clone(), db_txn, keypair, self.resources.clone());
        f(store)
    }
}
