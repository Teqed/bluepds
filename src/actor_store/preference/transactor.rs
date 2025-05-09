//! Transactor for preference operations.


use super::reader::PreferenceReader;

/// Transactor for preference operations.
pub(crate) struct PreferenceTransactor {
    /// Preference reader.
    pub reader: PreferenceReader,
}
