//! Filesystem path validation.

use std::io;
use std::path::Path;
use std::path::PathBuf;

/// Resolves an absolute input path within a canonical input root.
///
/// # Returns
///
/// The canonical path on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * The path is relative, escapes `root`, or cannot be represented in a line-based manifest.
/// * Forwards [`Path::canonicalize`]'s return values on failure.
pub fn resolve_input_path(root: &Path, path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input path must be absolute",
        ));
    }
    let resolved = path.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to resolve input {}: {error}", path.display()),
        )
    })?;
    if !resolved.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "input path {} is outside {}",
                path.display(),
                root.display()
            ),
        ));
    }
    if resolved
        .to_str()
        .is_none_or(|value| value.contains(['\n', '\r']))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "input path must be UTF-8 without line breaks",
        ));
    }
    Ok(resolved)
}
