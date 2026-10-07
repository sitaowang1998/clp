#ifndef CLP_S_AGGREGATIONSINK_HPP
#define CLP_S_AGGREGATIONSINK_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <bsoncxx/document/value.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/aggregators.hpp>

namespace clp_s {
/**
 * Consumes an aggregation's result documents and writes them to a destination.
 */
class AggregationSink {
public:
    // Constructors
    AggregationSink() = default;

    // Delete copy constructor and assignment operator
    AggregationSink(AggregationSink const&) = delete;
    auto operator=(AggregationSink const&) -> AggregationSink& = delete;

    // Delete move constructor and assignment operator
    AggregationSink(AggregationSink&&) = delete;
    auto operator=(AggregationSink&&) -> AggregationSink& = delete;

    // Destructor
    virtual ~AggregationSink() = default;

    // Methods
    /**
     * Writes one result document.
     * @param result The result document to write.
     * @return A void result on success, or an error code indicating the failure.
     */
    [[nodiscard]] virtual auto write(AggregationResult const& result)
            -> ystdlib::error_handling::Result<void>
            = 0;

    /**
     * Flushes any buffered results.
     * @return A void result on success, or an error code indicating the failure.
     */
    [[nodiscard]] virtual auto finish() -> ystdlib::error_handling::Result<void> = 0;
};

/**
 * Sink that writes aggregation results to standard output as newline-delimited JSON.
 */
class StdoutSink : public AggregationSink {
public:
    // Constructors
    explicit StdoutSink(std::string_view archive_id) : m_archive_id{archive_id} {}

    // Methods implementing AggregationSink
    /**
     * Dumps the document to stdout.
     * @param result The result document to write.
     * @return A void result on success, there is no error case.
     */
    [[nodiscard]] auto write(AggregationResult const& result)
            -> ystdlib::error_handling::Result<void> override;

    [[nodiscard]] auto finish() -> ystdlib::error_handling::Result<void> override {
        return ystdlib::error_handling::success();
    }

private:
    // Data members
    std::string m_archive_id;
};

/**
 * Sink that writes aggregation results to a MongoDB results-cache collection.
 */
class ResultsCacheSink : public AggregationSink {
public:
    // Constructors
    /**
     * Creates a sink for per-archive aggregation results.
     *
     * @param uri
     * @param collection
     * @param batch_size
     * @param archive_id
     * @param dataset
     * @param replace_time_buckets Whether to replace contributions keyed by dataset, archive,
     * and timestamp. Each result must contain the final count for that archive's bucket, and
     * retries must use identical inputs.
     * @throw std::invalid_argument if replacement writes use unacknowledged write concern.
     * @throw ResultsCacheConnectionError if connecting to the results cache fails.
     */
    ResultsCacheSink(
            std::string_view uri,
            std::string_view collection,
            uint64_t batch_size,
            std::string_view archive_id,
            std::string_view dataset,
            bool replace_time_buckets
    );

    // Methods implementing AggregationSink
    /**
     * Buffers a result document, flushing the buffer to the database once it reaches the batch
     * size.
     * @param result The result document to write.
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `flush_buffer`'s return values on failure.
     */
    [[nodiscard]] auto write(AggregationResult const& result)
            -> ystdlib::error_handling::Result<void> override;

    /**
     * Flushes any remaining buffered result documents.
     * @return A void result on success, or an error code indicating the failure:
     * - Forwards `flush_buffer`'s return values on failure.
     */
    [[nodiscard]] auto finish() -> ystdlib::error_handling::Result<void> override {
        return flush_buffer();
    }

private:
    // Methods
    /**
     * Persists the buffered documents, replacing time-bucket contributions when configured.
     * @return A void result on success, or an error code indicating the failure:
     * - std::errc::io_error if flushing failed.
     */
    [[nodiscard]] auto flush_buffer() -> ystdlib::error_handling::Result<void>;

    // Data members
    mongocxx::client m_client;
    mongocxx::collection m_collection;
    uint64_t m_batch_size;
    std::string m_archive_id;
    std::string m_dataset;
    bool m_replace_time_buckets;
    std::vector<bsoncxx::document::value> m_results;
};
}  // namespace clp_s

#endif  // CLP_S_AGGREGATIONSINK_HPP
