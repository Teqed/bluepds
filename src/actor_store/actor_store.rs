use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use super::ActorDb;
use super::actor_store_reader::ActorStoreReader;
use super::actor_store_resources::ActorStoreResources;
use super::actor_store_transactor::ActorStoreTransactor;
use super::actor_store_writer::ActorStoreWriter;
use crate::SigningKey;

pub(crate) struct ActorStore {
    pub(crate) directory: PathBuf,
    reserved_key_dir: PathBuf,
    resources: ActorStoreResources,
}

struct ActorLocation {
    directory: PathBuf,
    db_location: PathBuf,
    key_location: PathBuf,
}

impl ActorStore {
    pub(crate) fn new(directory: impl Into<PathBuf>, resources: ActorStoreResources) -> Self {
        let directory = directory.into();
        let reserved_key_dir = directory.join("reserved_keys");
        Self {
            directory,
            reserved_key_dir,
            resources,
        }
    }

    pub(crate) async fn get_location(&self, did: &str) -> Result<ActorLocation> {
        // const didHash = await crypto.sha256Hex(did)
        // const directory = path.join(this.cfg.directory, didHash.slice(0, 2), did)
        // const dbLocation = path.join(directory, `store.sqlite`)
        // const keyLocation = path.join(directory, `key`)
        // return { directory, dbLocation, keyLocation }
        todo!()
    }

    pub(crate) async fn exists(&self, did: &str) -> Result<bool> {
        // const location = await this.getLocation(did)
        // return await fileExists(location.dbLocation)
        todo!()
    }

    pub(crate) async fn keypair(&self, did: &str) -> Result<Arc<SigningKey>> {
        // const { keyLocation } = await this.getLocation(did)
        // const privKey = await fs.readFile(keyLocation)
        // return crypto.Secp256k1Keypair.import(privKey)
        todo!()
    }

    pub(crate) async fn open_db(&self, did: &str) -> Result<ActorDb> {
        // const { dbLocation } = await this.getLocation(did)
        // const exists = await fileExists(dbLocation)
        // if (!exists) {
        //   throw new InvalidRequestError('Repo not found', 'NotFound')
        // }

        // const db = getDb(dbLocation, this.cfg.disableWalAutoCheckpoint)

        // // run a simple select with retry logic to ensure the db is ready (not in wal recovery mode)
        // try {
        //   await retrySqlite(() =>
        //     db.db.selectFrom('repo_root').selectAll().execute(),
        //   )
        // } catch (err) {
        //   db.close()
        //   throw err
        // }

        // return db
        todo!()
    }

    pub(crate) async fn read<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreReader) -> Result<T>,
    {
        // const db = await this.openDb(did)
        // try {
        //   const getKeypair = () => this.keypair(did)
        //   return await fn(new ActorStoreReader(did, db, this.resources, getKeypair))
        // } finally {
        //   db.close()
        // }
        todo!()
    }

    pub(crate) async fn transact<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T>,
    {
        // const keypair = await this.keypair(did)
        // const db = await this.openDb(did)
        // try {
        //   return await db.transaction((dbTxn) => {
        //     return fn(new ActorStoreTransactor(did, dbTxn, keypair, this.resources))
        //   })
        // } finally {
        //   db.close()
        // }
        todo!()
    }

    pub(crate) async fn write_no_transaction<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreWriter) -> Result<T>,
    {
        // const keypair = await this.keypair(did)
        // const db = await this.openDb(did)
        // try {
        //   return await fn(new ActorStoreWriter(did, db, keypair, this.resources))
        // } finally {
        //   db.close()
        // }
        todo!()
    }

    pub(crate) async fn create(&self, did: &str, keypair: SigningKey) -> Result<()> {
        // const { directory, dbLocation, keyLocation } = await this.getLocation(did)
        // // ensure subdir exists
        // await mkdir(directory, { recursive: true })
        // const exists = await fileExists(dbLocation)
        // if (exists) {
        //   throw new InvalidRequestError('Repo already exists', 'AlreadyExists')
        // }
        // const privKey = await keypair.export()
        // await fs.writeFile(keyLocation, privKey)

        // const db: ActorDb = getDb(dbLocation, this.cfg.disableWalAutoCheckpoint)
        // try {
        //   await db.ensureWal()
        //   const migrator = getMigrator(db)
        //   await migrator.migrateToLatestOrThrow()
        // } finally {
        //   db.close()
        // }
        todo!()
    }

    pub(crate) async fn destroy(&self, did: &str) -> Result<()> {
        // const blobstore = this.resources.blobstore(did)
        // if (blobstore instanceof DiskBlobStore) {
        //   await blobstore.deleteAll()
        // } else {
        //   const cids = await this.read(did, async (store) =>
        //     store.repo.blob.getBlobCids(),
        //   )
        //   await Promise.allSettled(
        //     chunkArray(cids, 500).map((chunk) => blobstore.deleteMany(chunk)),
        //   )
        // }

        // const { directory } = await this.getLocation(did)
        // await rmIfExists(directory, true)
        todo!()
    }

    // async reserveKeypair(did?: string): Promise<string> {
    //   let keyLoc: string | undefined
    //   if (did) {
    //     assertSafePathPart(did)
    //     keyLoc = path.join(this.reservedKeyDir, did)
    //     const maybeKey = await loadKey(keyLoc)
    //     if (maybeKey) {
    //       return maybeKey.did()
    //     }
    //   }
    //   const keypair = await crypto.Secp256k1Keypair.create({ exportable: true })
    //   const keyDid = keypair.did()
    //   keyLoc = keyLoc ?? path.join(this.reservedKeyDir, keyDid)
    //   await mkdir(this.reservedKeyDir, { recursive: true })
    //   await fs.writeFile(keyLoc, await keypair.export())
    //   return keyDid
    // }

    // async getReservedKeypair(
    //   signingKeyOrDid: string,
    // ): Promise<ExportableKeypair | undefined> {
    //   return loadKey(path.join(this.reservedKeyDir, signingKeyOrDid))
    // }

    // async clearReservedKeypair(keyDid: string, did?: string) {
    //   await rmIfExists(path.join(this.reservedKeyDir, keyDid))
    //   if (did) {
    //     await rmIfExists(path.join(this.reservedKeyDir, did))
    //   }
    // }

    // async storePlcOp(did: string, op: Uint8Array) {
    //   const { directory } = await this.getLocation(did)
    //   const opLoc = path.join(directory, `did-op`)
    //   await fs.writeFile(opLoc, op)
    // }

    // async getPlcOp(did: string): Promise<Uint8Array> {
    //   const { directory } = await this.getLocation(did)
    //   const opLoc = path.join(directory, `did-op`)
    //   return await fs.readFile(opLoc)
    // }

    // async clearPlcOp(did: string) {
    //   const { directory } = await this.getLocation(did)
    //   const opLoc = path.join(directory, `did-op`)
    //   await rmIfExists(opLoc)
    // }
}

// const loadKey = async (loc: string): Promise<ExportableKeypair | undefined> => {
//   const privKey = await readIfExists(loc)
//   if (!privKey) return undefined
//   return crypto.Secp256k1Keypair.import(privKey, { exportable: true })
// }

// function assertSafePathPart(part: string) {
//   const normalized = path.normalize(part)
//   assert(
//     part === normalized &&
//       !part.startsWith('.') &&
//       !part.includes('/') &&
//       !part.includes('\\'),
//     `unsafe path part: ${part}`,
//   )
// }
