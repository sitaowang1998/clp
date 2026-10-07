//! Timeline bucket validation errors.

/// A source contribution or final bucket violates the integral timeline representation.
#[derive(Debug, thiserror::Error)]
#[error("malformed timeline bucket or overflowing count")]
pub struct Error;
