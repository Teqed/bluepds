use std::{path::PathBuf, str::FromStr as _};

use anyhow::Result;
use cidv10::Cid;
use rsky_common::get_random_str;

use crate::db::DatabaseConnection;

/// Type for stream of blob data
pub type BlobStream = Box<dyn std::io::Read + Send>;

/// Placeholder implementation for blob store
#[derive(Clone)]
pub(crate) struct BlobStoreSql {
    client: DatabaseConnection,
    path: PathBuf,
}

/// Configuration for the blob store
/// TODO: Implement this placeholder
pub(crate) struct BlobConfig {
    pub(crate) path: PathBuf,
}

/// ByteStream
/// TODO: Implement this placeholder
pub(crate) struct ByteStream {
    pub(crate) bytes: Vec<u8>,
}
impl ByteStream {
    pub async fn collect(self) -> Result<Vec<u8>> {
        Ok(self.bytes)
    }
}

impl BlobStoreSql {
    pub fn new(did: String, cfg: &BlobConfig) -> Self {
        // let client = aws_sdk_s3::Client::new(cfg);
        // BlobStorePlaceholder {
        //     client,
        //     bucket: did,
        // }
        todo!();
    }

    pub fn creator(cfg: &BlobConfig) -> Box<dyn Fn(String) -> BlobStoreSql + '_> {
        Box::new(move |did: String| BlobStoreSql::new(did, cfg))
    }

    fn gen_key(&self) -> String {
        get_random_str()
    }

    fn get_tmp_path(&self, key: &String) -> String {
        // format!("tmp/{0}/{1}", self.bucket, key)
        todo!();
    }

    fn get_stored_path(&self, cid: Cid) -> String {
        // format!("blocks/{0}/{1}", self.bucket, cid)
        todo!();
    }

    fn get_quarantined_path(&self, cid: Cid) -> String {
        // format!("quarantine/{0}/{1}", self.bucket, cid)
        todo!();
    }

    pub async fn put_temp(&self, bytes: Vec<u8>) -> Result<String> {
        let key = self.gen_key();
        // let body = ByteStream::from(bytes);
        // self.client
        //     .put_object()
        //     .body(body)
        //     .bucket(&self.bucket)
        //     .key(self.get_tmp_path(&key))
        //     .acl(ObjectCannedAcl::PublicRead)
        //     .send()
        //     .await?;
        // Ok(key)
        todo!();
    }

    pub async fn make_permanent(&self, key: String, cid: Cid) -> Result<()> {
        // let already_has = self.has_stored(cid).await?;
        // if !already_has {
        //     Ok(self
        //         .move_object(MoveObject {
        //             from: self.get_tmp_path(&key),
        //             to: self.get_stored_path(cid),
        //         })
        //         .await?)
        // } else {
        //     // already saved, so we no-op & just delete the temp
        //     Ok(self.delete_key(self.get_tmp_path(&key)).await?)
        // }
        todo!();
    }

    pub async fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> Result<()> {
        // let body = ByteStream::from(bytes);
        // self.client
        //     .put_object()
        //     .body(body)
        //     .bucket(&self.bucket)
        //     .key(self.get_stored_path(cid))
        //     .acl(ObjectCannedAcl::PublicRead)
        //     .send()
        //     .await?;
        // Ok(())
        todo!();
    }

    pub async fn quarantine(&self, cid: Cid) -> Result<()> {
        // self.move_object(MoveObject {
        //     from: self.get_stored_path(cid),
        //     to: self.get_quarantined_path(cid),
        // })
        // .await
        todo!();
    }

    pub async fn unquarantine(&self, cid: Cid) -> Result<()> {
        // self.move_object(MoveObject {
        //     from: self.get_quarantined_path(cid),
        //     to: self.get_stored_path(cid),
        // })
        // .await
        todo!();
    }

    async fn get_object(&self, cid: Cid) -> Result<ByteStream> {
        // let res = self
        //     .client
        //     .get_object()
        //     .bucket(&self.bucket)
        //     .key(self.get_stored_path(cid))
        //     .send()
        //     .await;
        // match res {
        //     Ok(res) => Ok(res.body),
        //     Err(SdkError::ServiceError(s)) => Err(anyhow::Error::new(s.into_err())),
        //     Err(e) => Err(anyhow::Error::new(e.into_service_error())),
        // }
        todo!();
    }

    pub async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>> {
        let res = self.get_object(cid).await?;
        // let bytes = res.collect().await.map(|data| data.into_bytes())?;
        // Ok(bytes.to_vec())
        todo!();
    }

    pub async fn get_stream(&self, cid: Cid) -> Result<ByteStream> {
        self.get_object(cid).await
    }

    pub async fn delete(&self, cid: String) -> Result<()> {
        self.delete_key(self.get_stored_path(Cid::from_str(&cid)?))
            .await
    }

    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        let keys: Vec<String> = cids
            .into_iter()
            .map(|cid| self.get_stored_path(cid))
            .collect();
        self.delete_many_keys(keys).await
    }

    pub async fn has_stored(&self, cid: Cid) -> Result<bool> {
        Ok(self.has_key(self.get_stored_path(cid)).await)
    }

    pub async fn has_temp(&self, key: String) -> Result<bool> {
        Ok(self.has_key(self.get_tmp_path(&key)).await)
    }

    async fn has_key(&self, key: String) -> bool {
        // let res = self
        //     .client
        //     .head_object()
        //     .bucket(&self.bucket)
        //     .key(key)
        //     .send()
        //     .await;
        // res.is_ok()
        todo!();
    }

    async fn delete_key(&self, key: String) -> Result<()> {
        // self.client
        //     .delete_object()
        //     .bucket(&self.bucket)
        //     .key(key)
        //     .send()
        //     .await?;
        // Ok(())
        todo!();
    }

    async fn delete_many_keys(&self, keys: Vec<String>) -> Result<()> {
        // let objects: Vec<ObjectIdentifier> = keys
        //     .into_iter()
        //     .map(|key| Ok(ObjectIdentifier::builder().key(key).build()?))
        //     .collect::<Result<Vec<ObjectIdentifier>>>()?;
        // let deletes = Delete::builder().set_objects(Some(objects)).build()?;
        // self.client
        //     .delete_objects()
        //     .bucket(&self.bucket)
        //     .delete(deletes)
        //     .send()
        //     .await?;
        // Ok(())
        todo!();
    }

    async fn move_object(&self, keys: MoveObject) -> Result<()> {
        // self.client
        //     .copy_object()
        //     .bucket(&self.bucket)
        //     .copy_source(format!(
        //         "{0}/{1}/{2}",
        //         env_str("AWS_ENDPOINT_BUCKET").unwrap(),
        //         self.bucket,
        //         keys.from
        //     ))
        //     .key(keys.to)
        //     .acl(ObjectCannedAcl::PublicRead)
        //     .send()
        //     .await?;
        // self.client
        //     .delete_object()
        //     .bucket(&self.bucket)
        //     .key(keys.from)
        //     .send()
        //     .await?;
        // Ok(())
        todo!();
    }
}

struct MoveObject {
    from: String,
    to: String,
}
