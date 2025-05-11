//! Type-safe casting utilities.

use serde::{Serialize, de::DeserializeOwned};
use std::fmt;

/// Represents an ISO 8601 date string (e.g., "2023-01-01T12:00:00Z").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateISO(String);

impl DateISO {
    /// Converts a `chrono::DateTime<Utc>` to a `DateISO`.
    pub fn from_date(date: chrono::DateTime<chrono::Utc>) -> Self {
        Self(date.to_rfc3339())
    }

    /// Converts a `DateISO` back to a `chrono::DateTime<Utc>`.
    pub fn to_date(&self) -> Result<chrono::DateTime<chrono::Utc>, chrono::ParseError> {
        self.0.parse::<chrono::DateTime<chrono::Utc>>()
    }
}

impl fmt::Display for DateISO {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents a JSON-encoded string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonEncoded<T: Serialize>(String, std::marker::PhantomData<T>);

impl<T: Serialize> JsonEncoded<T> {
    /// Encodes a value into a JSON string.
    pub fn to_json(value: &T) -> Result<Self, serde_json::Error> {
        let json = serde_json::to_string(value)?;
        Ok(Self(json, std::marker::PhantomData))
    }

    /// Decodes a JSON string back into a value.
    pub fn from_json(json_str: &str) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_str(json_str)
    }

    /// Returns the underlying JSON string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Serialize> fmt::Display for JsonEncoded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_date_iso() {
        let now = Utc::now();
        let date_iso = DateISO::from_date(now);
        let parsed_date = date_iso.to_date().unwrap();
        assert_eq!(now.to_rfc3339(), parsed_date.to_rfc3339());
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestStruct {
        name: String,
        value: i32,
    }

    #[test]
    fn test_json_encoded() {
        let test_value = TestStruct {
            name: "example".to_string(),
            value: 42,
        };

        // Encode to JSON
        let encoded = JsonEncoded::to_json(&test_value).unwrap();
        assert_eq!(encoded.as_str(), r#"{"name":"example","value":42}"#);

        // Decode from JSON
        let decoded: TestStruct = JsonEncoded::from_json(encoded.as_str()).unwrap();
        assert_eq!(decoded, test_value);
    }
}
