//! Partitioning of discovered files and S3 objects into compression-task inputs.

use std::collections::VecDeque;
use std::path::Path;

use clp_rust_utils::clp_config::S3Config;
use clp_rust_utils::job_config::ClpIoConfig;
use clp_rust_utils::job_config::InputConfig;
use clp_rust_utils::s3::ObjectMetadata;
use clp_rust_utils::task_io::compression::CompressionInputSource;
use clp_rust_utils::task_io::compression::FsInputSource;
use clp_rust_utils::task_io::compression::S3InputSource;

use crate::Error;

/// Builds inputs for compression tasks.
///
/// The builder buffers input objects internally and groups them into partitions. Each partition
/// contains the inputs for a single compression task.
pub struct CompressionInputBuilder {
    buffer: Vec<FileMetadata>,
    partitioned_task_inputs: Vec<CompressionInputSource>,
    total_buffered_size: u64,
    target_archive_size: u64,
    buffer_size_to_trigger_partition: u64,
    s3_config: Option<S3Config>,
    empty_directories: Vec<String>,
}

impl CompressionInputBuilder {
    /// Factory function.
    ///
    /// # Returns
    ///
    /// A newly created [`CompressionInputBuilder`] with an empty buffer on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::UnsupportedInputConfig`] if `clp_io_config`'s input config is an
    ///   [`InputConfig::FsInputConfig`].
    pub fn new(clp_io_config: &ClpIoConfig) -> Result<Self, Error> {
        let target_archive_size = clp_io_config.output.target_archive_size;
        let s3_config = match &clp_io_config.input {
            InputConfig::FsInputConfig { .. } => return Err(Error::UnsupportedInputConfig),
            InputConfig::S3InputConfig { config } => config.s3_config.clone(),
            InputConfig::S3ObjectMetadataInputConfig { config } => config.s3_config.clone(),
        };

        Ok(Self::from_s3_config(s3_config, target_archive_size))
    }

    /// Adds an S3 object to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// * [`Error::UnsupportedInputConfig`] if the builder is configured for filesystem input.
    /// * [`Error::S3BucketMismatch`] if `metadata`'s bucket differs from the buffer's configured
    ///   bucket.
    /// * [`Error::S3KeyPrefixMismatch`] if `metadata`'s key doesn't start with the buffer's
    ///   configured key prefix.
    pub fn add(&mut self, metadata: ObjectMetadata) -> Result<(), Error> {
        let Some(config) = &self.s3_config else {
            return Err(Error::UnsupportedInputConfig);
        };
        if metadata.bucket != config.bucket {
            return Err(Error::S3BucketMismatch(
                String::from(config.bucket.clone()),
                String::from(metadata.bucket),
            ));
        }

        if !metadata
            .key
            .as_str()
            .starts_with(config.key_prefix.as_str())
        {
            return Err(Error::S3KeyPrefixMismatch(
                String::from(config.key_prefix.clone()),
                String::from(metadata.key),
            ));
        }

        self.add_file(String::from(metadata.key), metadata.size);
        Ok(())
    }

    /// Flushes any remaining buffered objects and consumes the builder, returning the completed
    /// compression task inputs.
    ///
    /// # Returns
    ///
    /// A vector of input sources, where each source contains the inputs for a single compression
    /// task.
    #[must_use]
    pub fn into_task_input_sources(mut self) -> Vec<CompressionInputSource> {
        self.flush();
        if !self.empty_directories.is_empty() {
            let empty_directories = std::mem::take(&mut self.empty_directories);
            for directories in empty_directories.chunks(MAX_PARTITION_ENTRIES) {
                self.partitioned_task_inputs
                    .push(CompressionInputSource::Fs(FsInputSource {
                        file_paths: Vec::new(),
                        empty_directories: directories.to_vec(),
                    }));
            }
        }
        self.partitioned_task_inputs
    }

    /// Creates an empty builder from the S3 input settings and target archive size.
    ///
    /// # Returns
    ///
    /// A newly created [`CompressionInputBuilder`] with an empty buffer.
    #[must_use]
    pub(crate) const fn from_s3_config(s3_config: S3Config, target_archive_size: u64) -> Self {
        Self {
            buffer: Vec::new(),
            partitioned_task_inputs: Vec::new(),
            total_buffered_size: 0,
            target_archive_size,
            buffer_size_to_trigger_partition: target_archive_size.saturating_mul(2),
            s3_config: Some(s3_config),
            empty_directories: Vec::new(),
        }
    }

    /// Creates a filesystem partition builder.
    ///
    /// # Returns
    ///
    /// An empty builder with the supplied target size and empty directories.
    pub(crate) const fn from_filesystem(
        target_archive_size: u64,
        empty_directories: Vec<String>,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            partitioned_task_inputs: Vec::new(),
            total_buffered_size: 0,
            target_archive_size,
            buffer_size_to_trigger_partition: target_archive_size.saturating_mul(2),
            s3_config: None,
            empty_directories,
        }
    }

    /// Adds a discovered file to the partition buffer.
    pub(crate) fn add_file(&mut self, path: String, size: u64) {
        let file = FileMetadata::new(path, size);
        self.total_buffered_size = self.total_buffered_size.saturating_add(file.estimated_size);
        self.buffer.push(file);
        if self.total_buffered_size >= self.buffer_size_to_trigger_partition
            || self.buffer.len() >= MAX_PARTITION_ENTRIES
        {
            self.partition(self.buffer.len() >= MAX_PARTITION_ENTRIES);
        }
    }

    /// Partitions all the objects in the current buffer.
    fn flush(&mut self) {
        self.partition(true);
    }

    /// Converts a non-empty partition into a compression task input and pushes it to the underlying
    /// task inputs buffer.
    fn push_input_source(&mut self, object_keys: Vec<String>) {
        if object_keys.is_empty() {
            return;
        }

        let input = if let Some(config) = &self.s3_config {
            CompressionInputSource::S3(S3InputSource {
                endpoint_url: config.endpoint_url.clone(),
                region_code: config.region_code.clone(),
                bucket: config.bucket.clone(),
                aws_authentication: config.aws_authentication.clone(),
                object_keys,
            })
        } else {
            CompressionInputSource::Fs(FsInputSource {
                file_paths: object_keys,
                empty_directories: Vec::new(),
            })
        };
        self.partitioned_task_inputs.push(input);
    }

    /// Partitions buffered objects into target-sized compression task inputs.
    ///
    /// When `flush_buffer` is false, objects that cannot fill another target-sized partition
    /// remain buffered. Otherwise, the final partial partition is also emitted.
    fn partition(&mut self, flush_buffer: bool) {
        if !flush_buffer && self.total_buffered_size < self.target_archive_size {
            return;
        }
        if self.buffer.is_empty() {
            return;
        }

        let mut rr_iterator = RoundRobinIterator::new(std::mem::take(&mut self.buffer));

        'partitioning: while flush_buffer || self.total_buffered_size >= self.target_archive_size {
            let mut partition = Vec::new();
            let mut partition_size = 0_u64;

            while partition_size < self.target_archive_size
                && partition.len() < MAX_PARTITION_ENTRIES
            {
                let Some(file) = rr_iterator.next() else {
                    self.push_input_source(partition);
                    break 'partitioning;
                };

                partition_size = partition_size.saturating_add(file.estimated_size);
                self.total_buffered_size =
                    self.total_buffered_size.saturating_sub(file.estimated_size);
                partition.push(file.path);
            }

            self.push_input_source(partition);
        }

        self.buffer = rr_iterator.into_flatten();
    }
}

const MAX_PARTITION_ENTRIES: usize = 4096;

/// An input path and its estimated uncompressed size.
struct FileMetadata {
    path: String,
    estimated_size: u64,
}

impl FileMetadata {
    /// Creates metadata for the object identified by `path`, estimating its uncompressed size from
    /// its raw `size`.
    ///
    /// # Returns
    ///
    /// A new [`FileMetadata`].
    fn new(path: String, size: u64) -> Self {
        let estimated_size = estimate_uncompressed_size(&path, size);
        Self {
            path,
            estimated_size,
        }
    }
}

/// Owns filename-similarity groups and returns one object from each group in round-robin order.
struct RoundRobinIterator {
    groups: VecDeque<std::vec::IntoIter<FileMetadata>>,
}

impl RoundRobinIterator {
    /// Groups objects by filename similarity and creates a round-robin iterator over the groups.
    ///
    /// # Returns
    ///
    /// A new [`RoundRobinIterator`].
    fn new(files: Vec<FileMetadata>) -> Self {
        let groups = group_files_by_similar_filenames(files)
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(IntoIterator::into_iter)
            .collect();

        Self { groups }
    }

    /// Consumes the iterator and collects every object that has not yet been returned.
    ///
    /// # Returns
    ///
    /// The remaining objects, flattened according to the iterator's current group order.
    fn into_flatten(self) -> Vec<FileMetadata> {
        self.groups.into_iter().flatten().collect()
    }
}

impl Iterator for RoundRobinIterator {
    type Item = FileMetadata;

    fn next(&mut self) -> Option<Self::Item> {
        let mut group = self.groups.pop_front()?;
        let file = group.next()?;
        if !group.as_slice().is_empty() {
            self.groups.push_back(group);
        }
        Some(file)
    }
}

/// Estimates an input's uncompressed size from its compressor suffix.
///
/// # Returns
///
/// The estimated uncompressed size.
fn estimate_uncompressed_size(key: &str, size: u64) -> u64 {
    const GZIP_COMPRESSION_RATIO_ESTIMATE: u64 = 13;
    const GZIP_SUFFIXES: &[&str] = &[".gz", ".gzip", ".tgz"];
    const ZSTD_COMPRESSION_RATIO_ESTIMATE: u64 = 8;

    if GZIP_SUFFIXES
        .iter()
        .any(|suffix| ends_with_ignore_ascii_case(key, suffix))
    {
        size.saturating_mul(GZIP_COMPRESSION_RATIO_ESTIMATE)
    } else if ends_with_ignore_ascii_case(key, ".zst") {
        size.saturating_mul(ZSTD_COMPRESSION_RATIO_ESTIMATE)
    } else {
        size
    }
}

/// # Returns
///
/// Whether `key` ends with `suffix` using ASCII case-insensitive match.
fn ends_with_ignore_ascii_case(key: &str, suffix: &str) -> bool {
    let suffix_start = key.len().saturating_sub(suffix.len());
    let key_suffix = key.get(suffix_start..);
    key_suffix.is_some_and(|key_suffix| key_suffix.eq_ignore_ascii_case(suffix))
}

/// Gets the filename portion of an input path.
///
/// # Returns
///
/// The key's filename, or the complete key if it has no filename component.
fn filename(key: &str) -> &str {
    let Some(file_name) = Path::new(key)
        .file_name()
        .and_then(|file_name| file_name.to_str())
    else {
        return key;
    };
    file_name
}

/// Groups input paths by filename similarity.
///
/// Places files with sufficiently similar filenames into the same group based on their normalized
/// Levenshtein similarity.
///
/// # Returns
///
/// A vector of filename-similarity groups.
fn group_files_by_similar_filenames(mut files: Vec<FileMetadata>) -> Vec<Vec<FileMetadata>> {
    const FILE_GROUPING_MIN_LEVENSHTEIN_RATIO: f64 = 0.6;

    files.sort_by(|a, b| filename(&a.path).cmp(filename(&b.path)));
    files
        .into_iter()
        .fold(Vec::new(), |mut groups: Vec<Vec<FileMetadata>>, curr| {
            match groups.last_mut() {
                Some(group)
                    if group.last().is_some_and(|prev| {
                        strsim::normalized_levenshtein(filename(&prev.path), filename(&curr.path))
                            >= FILE_GROUPING_MIN_LEVENSHTEIN_RATIO
                    }) =>
                {
                    group.push(curr);
                }
                _ => groups.push(vec![curr]),
            }
            groups
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use clp_rust_utils::clp_config::AwsAuthentication;
    use clp_rust_utils::dataset::DatasetName;
    use clp_rust_utils::job_config::OutputConfig;
    use clp_rust_utils::job_config::S3InputConfig;
    use non_empty_string::NonEmptyString;

    use super::*;

    const TEST_BUCKET: &str = "test-bucket";

    fn create_non_empty_string(value: &str) -> NonEmptyString {
        NonEmptyString::try_from(value).expect("test string literals are non-empty")
    }

    fn create_builder(target_archive_size: u64) -> CompressionInputBuilder {
        let s3_config = S3Config {
            bucket: create_non_empty_string(TEST_BUCKET),
            region_code: None,
            key_prefix: create_non_empty_string("logs/"),
            endpoint_url: None,
            aws_authentication: AwsAuthentication::Default,
        };
        let clp_io_config = ClpIoConfig {
            input: InputConfig::S3InputConfig {
                config: S3InputConfig {
                    s3_config,
                    keys: None,
                    dataset: DatasetName::default(),
                    timestamp_key: None,
                    unstructured: false,
                },
            },
            output: OutputConfig {
                target_archive_size,
                target_dictionaries_size: 1024,
                target_encoded_file_size: 1024,
                target_segment_size: 1024,
                compression_level: 3,
            },
        };

        CompressionInputBuilder::new(&clp_io_config).expect("S3 input config should be supported")
    }

    fn add_object(
        builder: &mut CompressionInputBuilder,
        key_to_size: &mut BTreeMap<String, u64>,
        key: &str,
        size: u64,
    ) {
        builder
            .add(ObjectMetadata {
                bucket: create_non_empty_string(TEST_BUCKET),
                key: create_non_empty_string(key),
                size,
            })
            .expect("object's bucket matches the builder's configured bucket");
        key_to_size.insert(String::from(key), size);
    }

    fn assert_partition_invariants(
        input_sources: &[CompressionInputSource],
        key_to_size: &BTreeMap<String, u64>,
        target_archive_size: u64,
    ) {
        let mut key_counts: BTreeMap<&str, usize> = BTreeMap::new();
        for key in input_sources
            .iter()
            .flat_map(clp_rust_utils::task_io::compression::CompressionInputSource::paths)
        {
            *key_counts.entry(key.as_str()).or_default() += 1;
        }
        for key in key_to_size.keys() {
            assert_eq!(key_counts.remove(key.as_str()), Some(1));
        }
        assert!(key_counts.is_empty());

        for source in input_sources {
            let total_size: u64 = source
                .paths()
                .iter()
                .map(|key| key_to_size.get(key).expect("every key was added"))
                .sum();
            let last_size = source
                .paths()
                .last()
                .map(|key| key_to_size.get(key).expect("every key was added"))
                .expect("`push_input_source` never pushes an empty partition");

            // Files are appended only while the partition is still under the target, so removing
            // the last-appended file must drop the partition back below the target.
            assert!(total_size - last_size < target_archive_size);
        }
    }

    #[test]
    fn filesystem_manifests_bound_zero_size_files_and_empty_directories() {
        let count = MAX_PARTITION_ENTRIES + 17;
        let directories: Vec<_> = (0..count)
            .map(|index| format!("/logs/empty/{index}"))
            .collect();
        let mut builder = CompressionInputBuilder::from_filesystem(1024, directories.clone());
        for index in 0..count {
            builder.add_file(format!("/logs/file/{index}"), 0);
        }
        let inputs = builder.into_task_input_sources();
        let mut all_files = Vec::new();
        let mut all_directories = Vec::new();
        for input in inputs {
            let CompressionInputSource::Fs(source) = input else {
                panic!("expected filesystem manifest")
            };
            assert!(
                source.file_paths.len() + source.empty_directories.len() <= MAX_PARTITION_ENTRIES
            );
            all_files.extend(source.file_paths);
            all_directories.extend(source.empty_directories);
        }
        assert_eq!(all_files.len(), count);
        all_files.sort();
        all_files.dedup();
        assert_eq!(all_files.len(), count);
        assert_eq!(all_directories, directories);
    }

    #[test]
    fn test_estimate_uncompressed_size_for_gzip_suffix() {
        const FILE_SIZE: u64 = 100;

        for path in [
            "logs/app.log.gz",
            "logs/app.log.GZ",
            "logs/app.log.gzip",
            "logs/app.log.GZIP",
            "logs/app.log.tgz",
            "logs/app.log.TGZ",
            "logs/app.log.tar.gz",
            "logs/app.log.TAR.GZ",
        ] {
            assert_eq!(FILE_SIZE * 13, estimate_uncompressed_size(path, FILE_SIZE));
        }
    }

    #[test]
    fn test_estimate_uncompressed_size_for_zstandard_suffix() {
        const FILE_SIZE: u64 = 100;

        for path in [
            "logs/app.log.zst",
            "logs/app.log.clp.zst",
            "logs/app.log.tar.zst",
            "logs/app.log.ZST",
        ] {
            assert_eq!(FILE_SIZE * 8, estimate_uncompressed_size(path, FILE_SIZE));
        }
    }

    #[test]
    fn test_partition_on_flush() {
        const TARGET_ARCHIVE_SIZE: u64 = 1000;
        const FILE_SIZE: u64 = 100;
        const NUM_FILES: u64 = 15;

        let mut builder = create_builder(TARGET_ARCHIVE_SIZE);
        let mut key_to_size = BTreeMap::new();
        for i in 0..NUM_FILES {
            add_object(
                &mut builder,
                &mut key_to_size,
                &format!("logs/app.log.{i}"),
                FILE_SIZE,
            );
        }

        assert_eq!(
            builder.partitioned_task_inputs,
            [] as [CompressionInputSource; 0]
        );

        let input_sources = builder.into_task_input_sources();
        assert_ne!(input_sources, [] as [CompressionInputSource; 0]);
        assert_partition_invariants(&input_sources, &key_to_size, TARGET_ARCHIVE_SIZE);
    }

    #[test]
    fn test_partition_on_the_fly() {
        const TARGET_ARCHIVE_SIZE: u64 = 1000;
        const FILE_STEMS: &[&str] = &["app.log", "database.log", "kernel.log", "nginx.log"];
        const NUM_FILES_PER_STEM: u64 = 60;

        let mut builder = create_builder(TARGET_ARCHIVE_SIZE);
        let mut key_to_size = BTreeMap::new();
        for stem in FILE_STEMS {
            for i in 0..NUM_FILES_PER_STEM {
                add_object(
                    &mut builder,
                    &mut key_to_size,
                    &format!("logs/{stem}.{i}"),
                    10 + (i % 7) * 30,
                );
            }
        }

        assert_ne!(
            builder.partitioned_task_inputs,
            [] as [CompressionInputSource; 0]
        );

        let input_sources = builder.into_task_input_sources();
        assert_partition_invariants(&input_sources, &key_to_size, TARGET_ARCHIVE_SIZE);
    }

    #[test]
    fn test_round_robin_iterator() {
        let files = ["app.log", "app.log.1", "app.log.2"]
            .into_iter()
            .chain(["nginx.log", "nginx.log.1", "nginx.log.2"])
            .map(|key| FileMetadata::new(String::from(key), 1))
            .collect();

        let mut rr_iterator = RoundRobinIterator::new(files);
        let yielded: Vec<String> = rr_iterator.by_ref().take(3).map(|file| file.path).collect();
        assert_eq!(yielded, vec!["app.log", "nginx.log", "app.log.1"]);

        let mut remaining: Vec<String> = rr_iterator
            .into_flatten()
            .into_iter()
            .map(|file| file.path)
            .collect();
        remaining.sort();
        assert_eq!(remaining, vec!["app.log.2", "nginx.log.1", "nginx.log.2"]);
    }
}
