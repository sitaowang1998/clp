#include "AggregationSink.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <variant>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/model/replace_one.hpp>
#include <mongocxx/write_concern.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <ystdlib/error_handling/Result.hpp>

#include <clp_s/archive_constants.hpp>
#include <clp_s/ResultsCacheUtils.hpp>

namespace clp_s {
using std::string_view;

auto StdoutSink::write(AggregationResult const& result) -> ystdlib::error_handling::Result<void> {
    nlohmann::json document;
    document[static_cast<char const*>(constants::results_cache::search::cArchiveId)] = m_archive_id;
    for (auto const& [key, value] : result) {
        std::visit([&](auto const& field_value) -> void { document[key] = field_value; }, value);
    }
    std::cout << document.dump() << '\n';
    return ystdlib::error_handling::success();
}

ResultsCacheSink::ResultsCacheSink(
        string_view uri,
        string_view collection,
        uint64_t batch_size,
        string_view archive_id,
        string_view dataset,
        bool replace_time_buckets
)
        : m_batch_size{batch_size},
          m_archive_id{archive_id},
          m_dataset{dataset},
          m_replace_time_buckets{replace_time_buckets} {
    m_collection = connect_to_results_cache(uri, collection, m_client);
    if (m_replace_time_buckets
        && mongocxx::write_concern::level::k_unacknowledged
                   == m_collection.write_concern().acknowledge_level())
    {
        throw std::invalid_argument("time-bucket writes require acknowledgment");
    }
}

auto ResultsCacheSink::flush_buffer() -> ystdlib::error_handling::Result<void> {
    if (m_results.empty()) {
        return ystdlib::error_handling::success();
    }

    try {
        if (m_replace_time_buckets) {
            auto writes{m_collection.create_bulk_write()};
            for (auto const& document : m_results) {
                bsoncxx::builder::basic::document filter;
                filter.append(
                        bsoncxx::builder::basic::kvp("_id", document.view()["_id"].get_document())
                );
                mongocxx::model::replace_one replacement{filter.extract(), document.view()};
                replacement.upsert(true);
                writes.append(replacement);
            }
            if (false == writes.execute().has_value()) {
                return std::errc::io_error;
            }
        } else {
            m_collection.insert_many(m_results);
        }
    } catch (mongocxx::exception const& e) {
        SPDLOG_ERROR("Failed to flush results to Results Cache: {}", e.what());
        return std::errc::io_error;
    }
    m_results.clear();
    return ystdlib::error_handling::success();
}

auto ResultsCacheSink::write(AggregationResult const& result)
        -> ystdlib::error_handling::Result<void> {
    bsoncxx::builder::basic::document document;
    document.append(
            bsoncxx::builder::basic::kvp(constants::results_cache::search::cArchiveId, m_archive_id)
    );
    for (auto const& [key, value] : result) {
        std::visit(
                [&](auto const& field_value) -> void {
                    document.append(bsoncxx::builder::basic::kvp(key, field_value));
                },
                value
        );
    }
    if (m_replace_time_buckets) {
        using bsoncxx::builder::basic::kvp;
        auto const timestamp{document.view()[static_cast<char const*>(
                                                     constants::results_cache::search::cTimestamp
                                             )]
                                     .get_int64()
                                     .value};
        bsoncxx::builder::basic::document identity;
        identity.append(
                kvp("dataset", m_dataset),
                kvp("archive_id", m_archive_id),
                kvp("timestamp", timestamp)
        );
        document.append(kvp("_id", identity.extract()), kvp("dataset", m_dataset));
    }
    m_results.push_back(document.extract());

    if (m_results.size() >= m_batch_size) {
        YSTDLIB_ERROR_HANDLING_TRYV(flush_buffer());
    }

    return ystdlib::error_handling::success();
}
}  // namespace clp_s
