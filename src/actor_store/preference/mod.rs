//! Preference handling for actor store.

mod reader;
mod transactor;

pub(crate) use reader::PreferenceReader;
pub(crate) use transactor::PreferenceTransactor;
