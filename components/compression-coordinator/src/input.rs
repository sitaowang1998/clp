//! Filesystem discovery for compression jobs.

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use clp_rust_utils::fs::resolve_input_path;
use clp_rust_utils::job_config::FsInputConfig;
use clp_rust_utils::task_io::compression::CompressionInputSource;

use crate::Error;
use crate::partition::CompressionInputBuilder;

/// Discovers and validates a filesystem job's complete input selection before partitioning it.
///
/// # Returns
///
/// Explicit task input manifests on success.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`Error::InvalidConfiguration`] if the input list or target archive size is empty.
/// * [`Error::Filesystem`] if a path is invalid, inaccessible, or not a regular file or directory.
/// * Forwards [`Path::canonicalize`]'s return values on failure.
/// * Forwards [`resolve_input_path`]'s return values on failure.
/// * Forwards [`std::fs::metadata`]'s return values on failure.
/// * Forwards [`std::fs::read_dir`]'s return values on failure.
pub fn discover_filesystem(
    config: &FsInputConfig,
    root: &Path,
    target_archive_size: u64,
) -> Result<Vec<CompressionInputSource>, Error> {
    if config.paths_to_compress.is_empty() || target_archive_size == 0 {
        return Err(Error::InvalidConfiguration(
            "filesystem compression requires input paths and a positive target archive size"
                .to_owned(),
        ));
    }
    let root = root.canonicalize()?;
    let mut pending: Vec<PathBuf> = config
        .paths_to_compress
        .iter()
        .map(|path| PathBuf::from(path.as_str()))
        .collect();
    let mut visited = HashSet::new();
    let mut files = Vec::new();
    let mut empty_directories = Vec::new();
    while let Some(path) = pending.pop() {
        let path = resolve_input_path(&root, &path)?;
        if !visited.insert(path.clone()) {
            continue;
        }
        let metadata = std::fs::metadata(&path)?;
        let path_string = path
            .to_str()
            .expect("resolved input paths are UTF-8")
            .to_owned();
        if metadata.is_file() {
            files.push((path_string, metadata.len()));
        } else if metadata.is_dir() {
            let entries = std::fs::read_dir(&path)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<Result<Vec<_>, _>>()?;
            if entries.is_empty() {
                empty_directories.push(path_string);
            } else {
                pending.extend(entries);
            }
        } else {
            return Err(Error::Filesystem(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "input {} is not a regular file or directory",
                    path.display()
                ),
            )));
        }
    }
    files.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    empty_directories.sort_unstable();
    let mut builder =
        CompressionInputBuilder::from_filesystem(target_archive_size, empty_directories);
    for (path, size) in files {
        builder.add_file(path, size);
    }
    let mut sources = builder.into_task_input_sources();
    if sources.is_empty() {
        sources.push(CompressionInputSource::Fs(
            clp_rust_utils::task_io::compression::FsInputSource {
                file_paths: Vec::new(),
                empty_directories: Vec::new(),
            },
        ));
    }
    Ok(sources)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::job_config::FsInputConfig;

    use super::discover_filesystem;

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        /// # Returns
        ///
        /// A fixture owning a unique temporary directory.
        fn new() -> Self {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos();
            let root = std::env::temp_dir()
                .join(format!("clp-fs-discovery-{}-{nonce}", std::process::id()));
            std::fs::create_dir(&root).expect("fixture directory is writable");
            Self { root }
        }

        /// # Returns
        ///
        /// A filesystem configuration selecting paths relative to the fixture.
        fn config(&self, paths: &[&str]) -> FsInputConfig {
            FsInputConfig {
                dataset: DatasetName::default(),
                paths_to_compress: paths
                    .iter()
                    .map(|path| {
                        self.root
                            .join(path)
                            .to_str()
                            .expect("fixture paths are UTF-8")
                            .try_into()
                            .expect("fixture paths are nonempty")
                    })
                    .collect(),
                path_prefix_to_remove: None,
                timestamp_key: None,
                unstructured: false,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).expect("fixture cleanup succeeds");
        }
    }

    #[test]
    fn recursive_discovery_includes_empty_files_and_deduplicates_overlaps() -> anyhow::Result<()> {
        let fixture = Fixture::new();
        std::fs::create_dir(fixture.root.join("nested"))?;
        std::fs::write(fixture.root.join("nested/data.json"), "{\"value\":1}\n")?;
        std::fs::write(fixture.root.join("empty.json"), "")?;
        let sources = discover_filesystem(
            &fixture.config(&[".", "nested/data.json"]),
            &fixture.root,
            1024,
        )?;
        let mut paths: Vec<_> = sources
            .iter()
            .flat_map(clp_rust_utils::task_io::compression::CompressionInputSource::paths)
            .cloned()
            .collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                fixture.root.join("empty.json").display().to_string(),
                fixture.root.join("nested/data.json").display().to_string()
            ]
        );
        Ok(())
    }

    #[test]
    fn discovery_rejects_escaping_symlinks_and_missing_paths() -> anyhow::Result<()> {
        let fixture = Fixture::new();
        let external = Fixture::new();
        std::fs::write(external.root.join("data"), "data")?;
        std::os::unix::fs::symlink(external.root.join("data"), fixture.root.join("link"))?;
        assert!(discover_filesystem(&fixture.config(&["link"]), &fixture.root, 1024).is_err());
        assert!(discover_filesystem(&fixture.config(&["missing"]), &fixture.root, 1024).is_err());
        Ok(())
    }

    #[test]
    fn discovery_resolves_internal_symlinks_and_terminates_cycles() -> anyhow::Result<()> {
        let fixture = Fixture::new();
        std::fs::write(fixture.root.join("data"), "data")?;
        std::os::unix::fs::symlink(fixture.root.join("data"), fixture.root.join("alias"))?;
        std::os::unix::fs::symlink(&fixture.root, fixture.root.join("cycle"))?;
        let sources = discover_filesystem(&fixture.config(&["."]), &fixture.root, 1024)?;
        let paths: Vec<_> = sources
            .iter()
            .flat_map(clp_rust_utils::task_io::compression::CompressionInputSource::paths)
            .collect();
        assert_eq!(
            paths,
            vec![&fixture.root.join("data").display().to_string()]
        );
        Ok(())
    }

    #[test]
    fn directory_cycle_without_files_produces_a_noop_task() -> anyhow::Result<()> {
        let fixture = Fixture::new();
        std::os::unix::fs::symlink(&fixture.root, fixture.root.join("cycle"))?;
        let sources = discover_filesystem(&fixture.config(&["."]), &fixture.root, 1024)?;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].paths(), &[] as &[String]);
        Ok(())
    }

    #[test]
    fn empty_directories_produce_an_explicit_manifest() -> anyhow::Result<()> {
        let fixture = Fixture::new();
        let sources = discover_filesystem(&fixture.config(&["."]), &fixture.root, 1024)?;
        let [clp_rust_utils::task_io::compression::CompressionInputSource::Fs(source)] =
            sources.as_slice()
        else {
            anyhow::bail!("expected one filesystem task");
        };
        assert_eq!(source.file_paths, Vec::<String>::new());
        assert_eq!(
            source.empty_directories,
            vec![fixture.root.display().to_string()]
        );
        Ok(())
    }
}
