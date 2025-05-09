//! Blob storage and retrieval for the actor store.

mod reader;
mod transactor;

pub use reader::BlobReader;
pub use transactor::{BlobMetadata, BlobTransactor};
