//! Protocol types exchanged with the Spider (Huntsman) tasks that run CLP query jobs.

use std::num::NonZeroU32;

use non_empty_string::NonEmptyString;
use serde::Deserialize;
use serde::Serialize;

use crate::job_config::SearchJobConfig;

/// `clp-s` options for a query job.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClpSQueryOption {
    /// The query string passed positionally to `clp-s`.
    pub query_string: NonEmptyString,

    /// The per-archive raw-result limit. When absent, the task uses the `clp-s` default.
    /// Timeline queries ignore this limit and count every matching record.
    pub max_num_results: Option<NonZeroU32>,

    /// Inclusive `--tge` bound in Unix epoch milliseconds.
    pub begin_timestamp_millisecs: Option<i64>,

    /// Inclusive `--tle` bound in Unix epoch milliseconds.
    pub end_timestamp_millisecs: Option<i64>,

    /// Whether `clp-s` performs a case-insensitive search.
    pub ignore_case: bool,

    /// Positive count-by-time bucket width in milliseconds. Absent for raw search.
    #[serde(default)]
    pub count_by_time_bucket_size_millisecs: Option<i64>,
}

impl TryFrom<&SearchJobConfig> for ClpSQueryOption {
    type Error = &'static str;

    /// Converts a search configuration to the options supported by Spider's CLP-S worker.
    ///
    /// # Returns
    ///
    /// Query task options on success. Timeline queries omit the raw-result limit.
    ///
    /// # Errors
    ///
    /// Returns a description if the query is empty, timestamp bounds are reversed, or the
    /// configuration requests unsupported filters, output, or aggregation settings.
    fn try_from(config: &SearchJobConfig) -> Result<Self, Self::Error> {
        if config.path_filter.is_some() || config.network_address.is_some() || config.write_to_file
        {
            return Err(
                "spider search does not support path filters, network output, or file output",
            );
        }
        if config
            .begin_timestamp
            .zip(config.end_timestamp)
            .is_some_and(|(begin, end)| begin > end)
        {
            return Err("begin timestamp must not exceed end timestamp");
        }
        let bucket_size = if let Some(aggregation) = &config.aggregation_config {
            if aggregation.do_count_aggregation == Some(true)
                || aggregation.job_id.is_some()
                || aggregation.reducer_host.is_some()
                || aggregation.reducer_port.is_some()
            {
                return Err("spider search does not support count aggregation or reducer settings");
            }
            let Some(bucket_size) = aggregation.count_by_time_bucket_size else {
                return Err("spider aggregation requires a count-by-time bucket size");
            };
            if bucket_size <= 0 {
                return Err("count-by-time bucket size must be positive");
            }
            Some(bucket_size)
        } else {
            None
        };
        Ok(Self {
            query_string: NonEmptyString::try_from(config.query_string.clone())
                .map_err(|_| "query string must not be empty")?,
            max_num_results: if bucket_size.is_some() {
                None
            } else {
                NonZeroU32::new(config.max_num_results)
            },
            begin_timestamp_millisecs: config.begin_timestamp,
            end_timestamp_millisecs: config.end_timestamp,
            ignore_case: config.ignore_case,
            count_by_time_bucket_size_millisecs: bucket_size,
        })
    }
}

/// The output handler that `clp-s` writes a query task's results to.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "type")]
pub enum OutputHandle {
    /// The results cache, addressed by a `MongoDB` URI whose path names the database. The
    /// collection is the query job's ID.
    #[serde(rename = "results_cache")]
    ResultsCache { uri: NonEmptyString },

    /// A file per archive. Not yet supported by the Spider query flow.
    #[serde(rename = "file")]
    File,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use non_empty_string::NonEmptyString;

    use super::ClpSQueryOption;
    use super::OutputHandle;
    use crate::types::non_empty_string::ExpectedNonEmpty;

    #[test]
    fn legacy_query_options_decode_from_map_and_sequence() -> anyhow::Result<()> {
        // Frozen pre-timeline payloads: ["*", 1, nil, nil, false] and its named-field form.
        let sequence = [0x95, 0xa1, b'*', 1, 0xc0, 0xc0, 0xc2];
        let map = b"\x85\xacquery_string\xa1*\xafmax_num_results\x01\
            \xb9begin_timestamp_millisecs\xc0\xb7end_timestamp_millisecs\xc0\
            \xabignore_case\xc2";
        for payload in [&sequence[..], &map[..]] {
            let options: ClpSQueryOption = rmp_serde::from_slice(payload)?;
            assert_eq!(options.query_string.as_str(), "*");
            assert_eq!(options.max_num_results.map(NonZeroU32::get), Some(1));
            assert_eq!(options.count_by_time_bucket_size_millisecs, None);
        }
        Ok(())
    }

    #[test]
    fn timeline_query_options_round_trip_and_remove_raw_limit() -> anyhow::Result<()> {
        let config = crate::job_config::SearchJobConfig {
            query_string: "*".to_owned(),
            max_num_results: 1,
            aggregation_config: Some(crate::job_config::AggregationConfig {
                count_by_time_bucket_size: Some(1_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let expected = ClpSQueryOption::try_from(&config).map_err(anyhow::Error::msg)?;
        assert_eq!(expected.count_by_time_bucket_size_millisecs, Some(1_000));
        assert_eq!(expected.max_num_results, None);
        for payload in [
            rmp_serde::to_vec(&expected)?,
            rmp_serde::to_vec_named(&expected)?,
        ] {
            let actual: ClpSQueryOption = rmp_serde::from_slice(&payload)?;
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[test]
    fn unsupported_search_config_is_rejected() -> anyhow::Result<()> {
        use crate::job_config::AggregationConfig;
        use crate::job_config::SearchJobConfig;

        let base = SearchJobConfig {
            query_string: "*".to_owned(),
            max_num_results: 7,
            ..Default::default()
        };
        assert_eq!(
            ClpSQueryOption::try_from(&base)
                .map_err(anyhow::Error::msg)?
                .max_num_results
                .map(NonZeroU32::get),
            Some(7)
        );
        for aggregation in [
            AggregationConfig::default(),
            AggregationConfig {
                count_by_time_bucket_size: Some(0),
                ..Default::default()
            },
            AggregationConfig {
                count_by_time_bucket_size: Some(-1),
                ..Default::default()
            },
            AggregationConfig {
                do_count_aggregation: Some(true),
                count_by_time_bucket_size: Some(1),
                ..Default::default()
            },
            AggregationConfig {
                reducer_host: Some("host".to_owned()),
                count_by_time_bucket_size: Some(1),
                ..Default::default()
            },
            AggregationConfig {
                reducer_port: Some(1),
                count_by_time_bucket_size: Some(1),
                ..Default::default()
            },
            AggregationConfig {
                job_id: Some(1),
                count_by_time_bucket_size: Some(1),
                ..Default::default()
            },
        ] {
            let config = SearchJobConfig {
                aggregation_config: Some(aggregation),
                ..base.clone()
            };
            assert!(
                ClpSQueryOption::try_from(&config).is_err(),
                "accepted {config:?}"
            );
        }
        for config in [
            SearchJobConfig {
                query_string: String::new(),
                ..base.clone()
            },
            SearchJobConfig {
                path_filter: Some("path".to_owned()),
                ..base.clone()
            },
            SearchJobConfig {
                network_address: Some(("host".to_owned(), 1)),
                ..base.clone()
            },
            SearchJobConfig {
                write_to_file: true,
                ..base.clone()
            },
            SearchJobConfig {
                begin_timestamp: Some(1),
                end_timestamp: Some(0),
                ..base
            },
        ] {
            assert!(
                ClpSQueryOption::try_from(&config).is_err(),
                "accepted {config:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn clp_s_query_option_with_timestamp_bounds_round_trips_through_msgpack() {
        let expected = ClpSQueryOption {
            query_string: NonEmptyString::from_static_str("level:error"),
            max_num_results: Some(NonZeroU32::new(1_000).expect("1,000 is nonzero")),
            begin_timestamp_millisecs: Some(1_700_000_000_001),
            end_timestamp_millisecs: Some(1_700_000_000_999),
            ignore_case: true,
            count_by_time_bucket_size_millisecs: None,
        };

        let serialized = rmp_serde::to_vec(&expected).expect("query options should serialize");
        let actual: ClpSQueryOption =
            rmp_serde::from_slice(&serialized).expect("query options should deserialize");

        assert_eq!(expected, actual);
    }

    #[test]
    fn clp_s_query_option_without_timestamp_bounds_round_trips_through_msgpack() {
        let expected = ClpSQueryOption {
            query_string: NonEmptyString::from_static_str("*"),
            max_num_results: Some(NonZeroU32::new(1).expect("1 is nonzero")),
            begin_timestamp_millisecs: None,
            end_timestamp_millisecs: None,
            ignore_case: false,
            count_by_time_bucket_size_millisecs: None,
        };

        let serialized = rmp_serde::to_vec(&expected).expect("query options should serialize");
        let actual: ClpSQueryOption =
            rmp_serde::from_slice(&serialized).expect("query options should deserialize");

        assert_eq!(expected, actual);
    }

    #[test]
    fn clp_s_query_option_without_max_num_results_round_trips_through_msgpack() {
        let expected = ClpSQueryOption {
            query_string: NonEmptyString::from_static_str("*"),
            max_num_results: None,
            begin_timestamp_millisecs: None,
            end_timestamp_millisecs: None,
            ignore_case: false,
            count_by_time_bucket_size_millisecs: None,
        };

        let serialized = rmp_serde::to_vec(&expected).expect("query options should serialize");
        let actual: ClpSQueryOption =
            rmp_serde::from_slice(&serialized).expect("query options should deserialize");

        assert_eq!(expected, actual);
    }

    #[test]
    fn output_handle_results_cache_round_trips_through_msgpack() {
        let expected = OutputHandle::ResultsCache {
            uri: NonEmptyString::from_static_str("mongodb://results-cache:27017/clp-query-results"),
        };

        let serialized = rmp_serde::to_vec(&expected).expect("output handle should serialize");
        let actual: OutputHandle =
            rmp_serde::from_slice(&serialized).expect("output handle should deserialize");

        assert_eq!(expected, actual);
    }

    #[test]
    fn output_handle_file_round_trips_through_msgpack() {
        let expected = OutputHandle::File;

        let serialized = rmp_serde::to_vec(&expected).expect("output handle should serialize");
        let actual: OutputHandle =
            rmp_serde::from_slice(&serialized).expect("output handle should deserialize");

        assert_eq!(expected, actual);
    }
}
