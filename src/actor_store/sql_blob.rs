//! SQL-based blob storage implementation
#![expect(
    clippy::pub_use,
    clippy::single_char_lifetime_names,
    unused_qualifications
)]
use anyhow::{Context, Result};
use cidv10::Cid;
use diesel::prelude::*;
use std::sync::Arc;

use crate::db::DbConn;

/// ByteStream implementation for blob data
pub struct ByteStream {
    pub bytes: Vec<u8>,
}

impl ByteStream {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub async fn collect(self) -> Result<Vec<u8>> {
        Ok(self.bytes)
    }
}

/// SQL-based implementation of blob storage
#[derive(Clone)]
pub struct BlobStoreSql {
    /// Database connection for metadata
    pub db: Arc<DbConn>,
    /// DID of the actor
    pub did: String,
}

/// Blob table structure for SQL operations
#[derive(Queryable, Insertable, Debug)]
#[diesel(table_name = blobs)]
struct BlobEntry {
    cid: String,
    did: String,
    data: Vec<u8>,
    size: i32,
    mime_type: String,
    quarantined: bool,
}

// Table definition for blobs
table! {
    blobs (cid, did) {
        cid -> Text,
        did -> Text,
        data -> Binary,
        size -> Integer,
        mime_type -> Text,
        quarantined -> Bool,
    }
}

impl BlobStoreSql {
    /// Create a new SQL-based blob store for the given DID
    pub fn new(did: String, db: Arc<DbConn>) -> Self {
        BlobStoreSql { db, did }
    }

    /// Create a factory function for blob stores
    pub fn creator(db: Arc<DbConn>) -> Box<dyn Fn(String) -> BlobStoreSql> {
        let db_clone = db.clone();
        Box::new(move |did: String| BlobStoreSql::new(did, db_clone.clone()))
    }

    /// Store a blob temporarily - now just stores permanently with a key returned for API compatibility
    pub async fn put_temp(&self, bytes: Vec<u8>) -> Result<String> {
        // Generate a unique key as a CID based on the data
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(&bytes);
        let key = hex::encode(digest);

        // Just store the blob directly
        self.put_permanent_with_mime(
            Cid::try_from(format!("bafy{}", key)).unwrap_or_else(|_| Cid::default()),
            bytes,
            "application/octet-stream".to_string(),
        )
        .await?;

        // Return the key for API compatibility
        Ok(key)
    }

    /// Make a temporary blob permanent - just a no-op for API compatibility
    pub async fn make_permanent(&self, _key: String, _cid: Cid) -> Result<()> {
        // No-op since we don't have temporary blobs anymore
        Ok(())
    }

    /// Store a blob with specific mime type
    pub async fn put_permanent_with_mime(
        &self,
        cid: Cid,
        bytes: Vec<u8>,
        mime_type: String,
    ) -> Result<()> {
        let cid_str = cid.to_string();
        let did_clone = self.did.clone();
        let bytes_len = bytes.len() as i32;

        // Store directly in the database
        self.db
            .run(move |conn| {
                let data_clone = bytes.clone();
                let entry = BlobEntry {
                    cid: cid_str.clone(),
                    did: did_clone.clone(),
                    data: bytes,
                    size: bytes_len,
                    mime_type,
                    quarantined: false,
                };

                diesel::insert_into(blobs::table)
                    .values(&entry)
                    .on_conflict((blobs::cid, blobs::did))
                    .do_update()
                    .set(blobs::data.eq(data_clone))
                    .execute(conn)
                    .context("Failed to insert blob data")
            })
            .await?;

        Ok(())
    }

    /// Store a blob directly as permanent
    pub async fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> Result<()> {
        self.put_permanent_with_mime(cid, bytes, "application/octet-stream".to_string())
            .await
    }

    /// Quarantine a blob
    pub async fn quarantine(&self, cid: Cid) -> Result<()> {
        let cid_str = cid.to_string();
        let did_clone = self.did.clone();

        // Update the quarantine flag in the database
        self.db
            .run(move |conn| {
                diesel::update(blobs::table)
                    .filter(blobs::cid.eq(&cid_str))
                    .filter(blobs::did.eq(&did_clone))
                    .set(blobs::quarantined.eq(true))
                    .execute(conn)
                    .context("Failed to quarantine blob")
            })
            .await?;

        Ok(())
    }

    /// Unquarantine a blob
    pub async fn unquarantine(&self, cid: Cid) -> Result<()> {
        let cid_str = cid.to_string();
        let did_clone = self.did.clone();

        // Update the quarantine flag in the database
        self.db
            .run(move |conn| {
                diesel::update(blobs::table)
                    .filter(blobs::cid.eq(&cid_str))
                    .filter(blobs::did.eq(&did_clone))
                    .set(blobs::quarantined.eq(false))
                    .execute(conn)
                    .context("Failed to unquarantine blob")
            })
            .await?;

        Ok(())
    }

    /// Get a blob as a stream
    pub async fn get_object(&self, blob_cid: Cid) -> Result<ByteStream> {
        use self::blobs::dsl::*;

        let cid_str = blob_cid.to_string();
        let did_clone = self.did.clone();

        // Get the blob data from the database
        let blob_data = self
            .db
            .run(move |conn| {
                blobs
                    .filter(self::blobs::cid.eq(&cid_str))
                    .filter(did.eq(&did_clone))
                    .filter(quarantined.eq(false))
                    .select(data)
                    .first::<Vec<u8>>(conn)
                    .optional()
                    .context("Failed to query blob data")
            })
            .await?;

        if let Some(bytes) = blob_data {
            Ok(ByteStream::new(bytes))
        } else {
            anyhow::bail!("Blob not found: {}", blob_cid)
        }
    }

    /// Get blob bytes
    pub async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>> {
        let stream = self.get_object(cid).await?;
        stream.collect().await
    }

    /// Get a blob as a stream
    pub async fn get_stream(&self, cid: Cid) -> Result<ByteStream> {
        self.get_object(cid).await
    }

    /// Delete a blob by CID string
    pub async fn delete(&self, blob_cid: String) -> Result<()> {
        use self::blobs::dsl::*;

        let did_clone = self.did.clone();

        // Delete from database
        self.db
            .run(move |conn| {
                diesel::delete(blobs)
                    .filter(self::blobs::cid.eq(&blob_cid))
                    .filter(did.eq(&did_clone))
                    .execute(conn)
                    .context("Failed to delete blob")
            })
            .await?;

        Ok(())
    }

    /// Delete multiple blobs by CID
    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        use self::blobs::dsl::*;

        let cid_strings: Vec<String> = cids.into_iter().map(|c| c.to_string()).collect();
        let did_clone = self.did.clone();

        // Delete all blobs in one operation
        self.db
            .run(move |conn| {
                diesel::delete(blobs)
                    .filter(self::blobs::cid.eq_any(cid_strings))
                    .filter(did.eq(&did_clone))
                    .execute(conn)
                    .context("Failed to delete multiple blobs")
            })
            .await?;

        Ok(())
    }

    /// Check if a blob is stored
    pub async fn has_stored(&self, blob_cid: Cid) -> Result<bool> {
        use self::blobs::dsl::*;

        let cid_str = blob_cid.to_string();
        let did_clone = self.did.clone();

        let exists = self
            .db
            .run(move |conn| {
                diesel::select(diesel::dsl::exists(
                    blobs
                        .filter(self::blobs::cid.eq(&cid_str))
                        .filter(did.eq(&did_clone)),
                ))
                .get_result::<bool>(conn)
                .context("Failed to check if blob exists")
            })
            .await?;

        Ok(exists)
    }

    /// Check if a temporary blob exists - now just checks if any blob exists with the key pattern
    pub async fn has_temp(&self, key: String) -> Result<bool> {
        // We don't have temporary blobs anymore, but for compatibility we'll check if
        // there's a blob with a similar CID pattern
        let temp_cid = Cid::try_from(format!("bafy{}", key)).unwrap_or_else(|_| Cid::default());
        self.has_stored(temp_cid).await
    }
}
