#include "JsonParser.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <stack>
#include <utility>
#include <vector>

#include <absl/container/flat_hash_map.h>
#include <boost/uuid/uuid_io.hpp>
#include <curl/curl.h>
#include <simdjson.h>
#include <spdlog/spdlog.h>

#include "../clp/ffi/ir_stream/decoding_methods.hpp"
#include "../clp/ffi/ir_stream/Deserializer.hpp"
#include "../clp/ffi/ir_stream/IrUnitType.hpp"
#include "../clp/ffi/ir_stream/protocol_constants.hpp"
#include "../clp/ffi/KeyValuePairLogEvent.hpp"
#include "../clp/ffi/SchemaTree.hpp"
#include "../clp/ffi/Value.hpp"
#include "../clp/ir/EncodedTextAst.hpp"
#include "../clp/NetworkReader.hpp"
#include "../clp/ReaderInterface.hpp"
#include "../clp/streaming_compression/zstd/Decompressor.hpp"
#include "../clp/time_types.hpp"
#include "archive_constants.hpp"
#include "ErrorCode.hpp"
#include "InputConfig.hpp"
#include "JsonFileIterator.hpp"
#include "JsonParser.hpp"
#include "search/ast/ColumnDescriptor.hpp"
#include "search/ast/SearchUtils.hpp"

using clp::ffi::ir_stream::Deserializer;
using clp::ffi::ir_stream::IRErrorCode;
using clp::ffi::KeyValuePairLogEvent;
using clp::UtcOffset;

namespace clp_s {
/**
 * Class that implements `clp::ffi::ir_stream::IrUnitHandlerReq` for Key-Value IR compression.
 */
class IrUnitHandler {
public:
    [[nodiscard]] auto handle_log_event(KeyValuePairLogEvent&& log_event) -> IRErrorCode {
        m_deserialized_log_event.emplace(std::move(log_event));
        return IRErrorCode::IRErrorCode_Success;
    }

    [[nodiscard]] static auto handle_utc_offset_change(
            [[maybe_unused]] UtcOffset utc_offset_old,
            [[maybe_unused]] UtcOffset utc_offset_new
    ) -> IRErrorCode {
        return IRErrorCode::IRErrorCode_Decode_Error;
    }

    [[nodiscard]] auto handle_schema_tree_node_insertion(
            [[maybe_unused]] bool is_auto_generated,
            [[maybe_unused]] clp::ffi::SchemaTree::NodeLocator schema_tree_node_locator,
            [[maybe_unused]] std::shared_ptr<clp::ffi::SchemaTree const> const& schema_tree
    ) -> IRErrorCode {
        return IRErrorCode::IRErrorCode_Success;
    }

    [[nodiscard]] auto handle_end_of_stream() -> IRErrorCode {
        m_is_complete = true;
        return IRErrorCode::IRErrorCode_Success;
    }

    [[nodiscard]] auto get_deserialized_log_event() const
            -> std::optional<KeyValuePairLogEvent> const& {
        return m_deserialized_log_event;
    }

    void clear() { m_is_complete = false; }

    [[nodiscard]] auto is_complete() const -> bool { return m_is_complete; }

private:
    std::optional<KeyValuePairLogEvent> m_deserialized_log_event;
    bool m_is_complete{false};
};

JsonParser::JsonParser(JsonParserOption const& option)
        : m_num_messages(0),
          m_target_encoded_size(option.target_encoded_size),
          m_max_document_size(option.max_document_size),
          m_timestamp_key(option.timestamp_key),
          m_structurize_arrays(option.structurize_arrays),
          m_record_log_order(option.record_log_order),
          m_input_paths(option.input_paths),
          m_network_auth(option.network_auth) {
    if (false == m_timestamp_key.empty()) {
        if (false
            == clp_s::search::ast::tokenize_column_descriptor(
                    m_timestamp_key,
                    m_timestamp_column,
                    m_timestamp_namespace
            ))
        {
            SPDLOG_ERROR("Can not parse invalid timestamp key: \"{}\"", m_timestamp_key);
            throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
        }

        // Unescape individual tokens to match unescaped JSON and confirm there are no wildcards in
        // the timestamp column.
        auto column = clp_s::search::ast::ColumnDescriptor::create_from_escaped_tokens(
                m_timestamp_column,
                m_timestamp_namespace
        );
        m_timestamp_column.clear();
        for (auto it = column->descriptor_begin(); it != column->descriptor_end(); ++it) {
            if (it->wildcard()) {
                SPDLOG_ERROR("Timestamp key can not contain wildcards: \"{}\"", m_timestamp_key);
                throw OperationFailed(ErrorCodeBadParam, __FILENAME__, __LINE__);
            }
            m_timestamp_column.push_back(it->get_token());
        }
    }

    m_archive_options.archives_dir = option.archives_dir;
    m_archive_options.compression_level = option.compression_level;
    m_archive_options.print_archive_stats = option.print_archive_stats;
    m_archive_options.single_file_archive = option.single_file_archive;
    m_archive_options.min_table_size = option.min_table_size;
    m_archive_options.id = m_generator();
    m_archive_options.authoritative_timestamp = m_timestamp_column;
    m_archive_options.authoritative_timestamp_namespace = m_timestamp_namespace;

    m_archive_writer = std::make_unique<ArchiveWriter>();
    m_archive_writer->open(m_archive_options);
}

void JsonParser::parse_obj_in_array(ondemand::object line, int32_t parent_node_id) {
    ondemand::object_iterator it = line.begin();
    if (it == line.end()) {
        m_current_schema.insert_unordered(parent_node_id);
        return;
    }

    std::stack<ondemand::object> object_stack;
    std::stack<ondemand::object_iterator> object_it_stack;

    std::stack<int32_t> node_id_stack;
    node_id_stack.push(parent_node_id);
    object_stack.push(std::move(line));
    object_it_stack.push(std::move(it));

    size_t object_start = m_current_schema.start_unordered_object(NodeType::Object);
    ondemand::field cur_field;
    ondemand::value cur_value;
    std::string_view cur_key;
    int32_t node_id;
    while (true) {
        while (false == object_stack.empty() && object_it_stack.top() == object_stack.top().end()) {
            object_stack.pop();
            node_id_stack.pop();
            object_it_stack.pop();
            if (false == object_it_stack.empty()) {
                ++object_it_stack.top();
            }
        }

        if (object_stack.empty()) {
            m_current_schema.end_unordered_object(object_start);
            return;
        }

        cur_field = *object_it_stack.top();
        cur_key = cur_field.unescaped_key(true);
        cur_value = cur_field.value();

        switch (cur_value.type()) {
            case ondemand::json_type::object: {
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::Object, cur_key);
                object_stack.push(std::move(cur_value.get_object()));
                object_it_stack.push(std::move(object_stack.top().begin()));
                if (object_it_stack.top() == object_stack.top().end()) {
                    m_current_schema.insert_unordered(node_id);
                    object_stack.pop();
                    object_it_stack.pop();
                } else {
                    node_id_stack.push(node_id);
                    continue;
                }
                break;
            }
            case ondemand::json_type::array: {
                node_id = m_archive_writer->add_node(
                        node_id_stack.top(),
                        NodeType::StructuredArray,
                        cur_key
                );
                parse_array(cur_value.get_array(), node_id);
                break;
            }
            case ondemand::json_type::number: {
                ondemand::number number_value = cur_value.get_number();
                if (true == number_value.is_double()) {
                    double double_value = number_value.get_double();
                    m_current_parsed_message.add_unordered_value(double_value);
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::Float, cur_key);
                } else {
                    int64_t i64_value;
                    if (number_value.is_uint64()) {
                        i64_value = static_cast<int64_t>(number_value.get_uint64());
                    } else {
                        i64_value = number_value.get_int64();
                    }
                    m_current_parsed_message.add_unordered_value(i64_value);
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::Integer, cur_key);
                }
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::string: {
                std::string_view value = cur_value.get_string(true);
                if (value.find(' ') != std::string::npos) {
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::ClpString, cur_key);
                } else {
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::VarString, cur_key);
                }
                m_current_parsed_message.add_unordered_value(value);
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::boolean: {
                bool value = cur_value.get_bool();
                m_current_parsed_message.add_unordered_value(value);
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::Boolean, cur_key);
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::null: {
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::NullValue, cur_key);
                m_current_schema.insert_unordered(node_id);
                break;
            }
        }

        ++object_it_stack.top();
    }
}

void JsonParser::parse_array(ondemand::array array, int32_t parent_node_id) {
    ondemand::array_iterator it = array.begin();
    if (it == array.end()) {
        m_current_schema.insert_unordered(parent_node_id);
        return;
    }

    size_t array_start = m_current_schema.start_unordered_object(NodeType::StructuredArray);
    ondemand::value cur_value;
    int32_t node_id;
    for (; it != array.end(); ++it) {
        cur_value = *it;

        switch (cur_value.type()) {
            case ondemand::json_type::object: {
                node_id = m_archive_writer->add_node(parent_node_id, NodeType::Object, "");
                parse_obj_in_array(std::move(cur_value.get_object()), node_id);
                break;
            }
            case ondemand::json_type::array: {
                node_id = m_archive_writer->add_node(parent_node_id, NodeType::StructuredArray, "");
                parse_array(std::move(cur_value.get_array()), node_id);
                break;
            }
            case ondemand::json_type::number: {
                ondemand::number number_value = cur_value.get_number();
                if (true == number_value.is_double()) {
                    double double_value = number_value.get_double();
                    m_current_parsed_message.add_unordered_value(double_value);
                    node_id = m_archive_writer->add_node(parent_node_id, NodeType::Float, "");
                } else {
                    int64_t i64_value;
                    if (number_value.is_uint64()) {
                        i64_value = static_cast<int64_t>(number_value.get_uint64());
                    } else {
                        i64_value = number_value.get_int64();
                    }
                    m_current_parsed_message.add_unordered_value(i64_value);
                    node_id = m_archive_writer->add_node(parent_node_id, NodeType::Integer, "");
                }
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::string: {
                std::string_view value = cur_value.get_string(true);
                if (value.find(' ') != std::string::npos) {
                    node_id = m_archive_writer->add_node(parent_node_id, NodeType::ClpString, "");
                } else {
                    node_id = m_archive_writer->add_node(parent_node_id, NodeType::VarString, "");
                }
                m_current_parsed_message.add_unordered_value(value);
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::boolean: {
                bool value = cur_value.get_bool();
                m_current_parsed_message.add_unordered_value(value);
                node_id = m_archive_writer->add_node(parent_node_id, NodeType::Boolean, "");
                m_current_schema.insert_unordered(node_id);
                break;
            }
            case ondemand::json_type::null: {
                node_id = m_archive_writer->add_node(parent_node_id, NodeType::NullValue, "");
                m_current_schema.insert_unordered(node_id);
                break;
            }
        }
    }
    m_current_schema.end_unordered_object(array_start);
}

void JsonParser::parse_line(ondemand::value line, int32_t parent_node_id, std::string const& key) {
    int32_t node_id;
    std::stack<ondemand::object> object_stack;
    std::stack<int32_t> node_id_stack;
    std::stack<ondemand::object_iterator> object_it_stack;

    ondemand::field cur_field;

    std::string_view cur_key = key;
    node_id_stack.push(parent_node_id);

    do {
        if (false == object_stack.empty()) {
            cur_field = *object_it_stack.top();
            cur_key = cur_field.unescaped_key(true);
            line = cur_field.value();
        }

        switch (line.type()) {
            case ondemand::json_type::object: {
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::Object, cur_key);
                object_stack.push(std::move(line.get_object()));
                auto objref = object_stack.top();
                auto it = ondemand::object_iterator(objref.begin());
                if (it == objref.end()) {
                    m_current_schema.insert_ordered(node_id);
                    object_stack.pop();
                    break;
                } else {
                    object_it_stack.push(it);
                    node_id_stack.push(node_id);
                    continue;
                }
            }
            case ondemand::json_type::array: {
                if (m_structurize_arrays) {
                    node_id = m_archive_writer->add_node(
                            node_id_stack.top(),
                            NodeType::StructuredArray,
                            cur_key
                    );
                    parse_array(std::move(line.get_array()), node_id);
                } else {
                    std::string value
                            = std::string(std::string_view(simdjson::to_json_string(line)));
                    node_id = m_archive_writer->add_node(
                            node_id_stack.top(),
                            NodeType::UnstructuredArray,
                            cur_key
                    );
                    m_current_parsed_message.add_value(node_id, value);
                    m_current_schema.insert_ordered(node_id);
                }
                break;
            }
            case ondemand::json_type::number: {
                NodeType type;
                ondemand::number number_value = line.get_number();
                auto const matches_timestamp
                        = m_archive_writer->matches_timestamp(node_id_stack.top(), cur_key);
                if (false == number_value.is_double()) {
                    // FIXME: should have separate integer and unsigned
                    // integer types to handle values greater than max int64
                    type = NodeType::Integer;
                } else {
                    type = NodeType::Float;
                }
                node_id = m_archive_writer->add_node(node_id_stack.top(), type, cur_key);

                if (type == NodeType::Integer) {
                    int64_t i64_value;
                    if (number_value.is_uint64()) {
                        i64_value = static_cast<int64_t>(number_value.get_uint64());
                    } else {
                        i64_value = line.get_int64();
                    }

                    m_current_parsed_message.add_value(node_id, i64_value);
                    if (matches_timestamp) {
                        m_archive_writer
                                ->ingest_timestamp_entry(m_timestamp_key, node_id, i64_value);
                    }
                } else {
                    double double_value = line.get_double();
                    m_current_parsed_message.add_value(node_id, double_value);
                    if (matches_timestamp) {
                        m_archive_writer
                                ->ingest_timestamp_entry(m_timestamp_key, node_id, double_value);
                    }
                }
                m_current_schema.insert_ordered(node_id);
                break;
            }
            case ondemand::json_type::string: {
                std::string_view value = line.get_string(true);
                auto const matches_timestamp
                        = m_archive_writer->matches_timestamp(node_id_stack.top(), cur_key);
                if (matches_timestamp) {
                    node_id = m_archive_writer->add_node(
                            node_id_stack.top(),
                            NodeType::DateString,
                            cur_key
                    );
                    uint64_t encoding_id{0};
                    epochtime_t timestamp = m_archive_writer->ingest_timestamp_entry(
                            m_timestamp_key,
                            node_id,
                            value,
                            encoding_id
                    );
                    m_current_parsed_message.add_value(node_id, encoding_id, timestamp);
                } else if (value.find(' ') != std::string::npos) {
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::ClpString, cur_key);
                    m_current_parsed_message.add_value(node_id, value);
                } else {
                    node_id = m_archive_writer
                                      ->add_node(node_id_stack.top(), NodeType::VarString, cur_key);
                    m_current_parsed_message.add_value(node_id, value);
                }

                m_current_schema.insert_ordered(node_id);
                break;
            }
            case ondemand::json_type::boolean: {
                bool value = line.get_bool();
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::Boolean, cur_key);
                m_current_parsed_message.add_value(node_id, value);
                m_current_schema.insert_ordered(node_id);
                break;
            }
            case ondemand::json_type::null: {
                node_id = m_archive_writer
                                  ->add_node(node_id_stack.top(), NodeType::NullValue, cur_key);
                m_current_schema.insert_ordered(node_id);
                break;
            }
        }

        if (object_stack.empty()) {
            break;
        }

        bool hit_end;
        do {
            hit_end = false;
            ++object_it_stack.top();
            if (object_it_stack.top() == object_stack.top().end()) {
                object_it_stack.pop();
                object_stack.pop();
                node_id_stack.pop();
                hit_end = true;
            }
        } while (false == object_it_stack.empty() && hit_end);
    } while (false == object_stack.empty());
}

bool JsonParser::parse() {
    auto archive_creator_id = boost::uuids::to_string(m_generator());
    for (auto const& path : m_input_paths) {
        auto reader{try_create_reader(path, m_network_auth)};
        if (nullptr == reader) {
            std::ignore = m_archive_writer->close();
            return false;
        }

        JsonFileIterator json_file_iterator(*reader, m_max_document_size);
        if (simdjson::error_code::SUCCESS != json_file_iterator.get_error()) {
            SPDLOG_ERROR(
                    "Encountered error - {} - while trying to parse {} after parsing 0 bytes",
                    simdjson::error_message(json_file_iterator.get_error()),
                    path.path
            );
            std::ignore = m_archive_writer->close();
            return false;
        }

        simdjson::ondemand::document_stream::iterator json_it;

        m_num_messages = 0;
        size_t bytes_consumed_up_to_prev_archive = 0;
        size_t bytes_consumed_up_to_prev_record = 0;

        size_t file_split_number{0ULL};
        int32_t log_event_idx_node_id{};
        auto initialize_fields_for_archive = [&]() -> bool {
            if (m_record_log_order) {
                log_event_idx_node_id
                        = add_metadata_field(constants::cLogEventIdxName, NodeType::DeltaInteger);
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cFilename},
                        path.path
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cFilename,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cFileSplitNumber},
                        file_split_number
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cFileSplitNumber,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cArchiveCreatorId},
                        archive_creator_id
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cArchiveCreatorId,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            return true;
        };
        if (false == initialize_fields_for_archive()) {
            std::ignore = m_archive_writer->close();
            return false;
        }
        auto update_fields_after_archive_split = [&]() { ++file_split_number; };

        while (json_file_iterator.get_json(json_it)) {
            m_current_schema.clear();

            auto ref = *json_it;
            auto is_scalar_result = ref.is_scalar();
            // If you don't check the error on is_scalar it will sometimes throw TAPE_ERROR when
            // converting to bool. The error being TAPE_ERROR or is_scalar() being true both mean
            // that this isn't a valid JSON document but they get set in different situations so we
            // need to check both here.
            if (is_scalar_result.error() || true == is_scalar_result.value()) {
                SPDLOG_ERROR(
                        "Encountered non-json-object while trying to parse {} after parsing {} "
                        "bytes",
                        path.path,
                        bytes_consumed_up_to_prev_record
                );
                std::ignore = m_archive_writer->close();
                return false;
            }

            // Add log_event_idx field to metadata for record
            if (m_record_log_order) {
                m_current_parsed_message.add_value(
                        log_event_idx_node_id,
                        m_archive_writer->get_next_log_event_id()
                );
                m_current_schema.insert_ordered(log_event_idx_node_id);
            }

            // Some errors from simdjson are latent until trying to access invalid JSON fields.
            // Instead of checking for an error every time we access a JSON field in parse_line we
            // just catch simdjson_error here instead.
            try {
                parse_line(ref.value(), constants::cRootNodeId, constants::cRootNodeName);
            } catch (simdjson::simdjson_error& error) {
                SPDLOG_ERROR(
                        "Encountered error - {} - while trying to parse {} after parsing {} bytes",
                        error.what(),
                        path.path,
                        bytes_consumed_up_to_prev_record
                );
                std::ignore = m_archive_writer->close();
                return false;
            }
            m_num_messages++;

            int32_t current_schema_id = m_archive_writer->add_schema(m_current_schema);
            m_current_parsed_message.set_id(current_schema_id);
            m_archive_writer
                    ->append_message(current_schema_id, m_current_schema, m_current_parsed_message);

            bytes_consumed_up_to_prev_record = json_file_iterator.get_num_bytes_consumed();
            if (m_archive_writer->get_data_size() >= m_target_encoded_size) {
                m_archive_writer->increment_uncompressed_size(
                        bytes_consumed_up_to_prev_record - bytes_consumed_up_to_prev_archive
                );
                bytes_consumed_up_to_prev_archive = bytes_consumed_up_to_prev_record;
                split_archive();
                update_fields_after_archive_split();
                if (false == initialize_fields_for_archive()) {
                    std::ignore = m_archive_writer->close();
                    return false;
                }
            }

            m_current_parsed_message.clear();
        }

        m_archive_writer->increment_uncompressed_size(
                json_file_iterator.get_num_bytes_read() - bytes_consumed_up_to_prev_archive
        );

        if (simdjson::error_code::SUCCESS != json_file_iterator.get_error()) {
            SPDLOG_ERROR(
                    "Encountered error - {} - while trying to parse {} after parsing {} bytes",
                    simdjson::error_message(json_file_iterator.get_error()),
                    path.path,
                    bytes_consumed_up_to_prev_record
            );
            std::ignore = m_archive_writer->close();
            return false;
        } else if (json_file_iterator.truncated_bytes() > 0) {
            // currently don't treat truncated bytes at the end of the file as an error
            SPDLOG_WARN(
                    "Truncated JSON  ({} bytes) at end of file {}",
                    json_file_iterator.truncated_bytes(),
                    path.path
            );
        }

        if (check_and_log_curl_error(path, reader)) {
            std::ignore = m_archive_writer->close();
            return false;
        }

        if (auto const rc = m_archive_writer->close_current_range(); ErrorCodeSuccess != rc) {
            SPDLOG_ERROR("Failed to close metadata range: {}", static_cast<int64_t>(rc));
            std::ignore = m_archive_writer->close();
            return false;
        }
    }
    return true;
}

int32_t JsonParser::add_metadata_field(std::string_view const field_name, NodeType type) {
    auto metadata_subtree_id = m_archive_writer->add_node(
            constants::cRootNodeId,
            NodeType::Metadata,
            constants::cMetadataSubtreeName
    );
    return m_archive_writer->add_node(metadata_subtree_id, type, field_name);
}

auto JsonParser::get_archive_node_type(
        clp::ffi::SchemaTree const& tree,
        std::pair<clp::ffi::SchemaTree::Node::id_t, std::optional<clp::ffi::Value>> const& kv_pair
) -> NodeType {
    clp::ffi::SchemaTree::Node const& tree_node = tree.get_node(kv_pair.first);
    clp::ffi::SchemaTree::Node::Type const ir_node_type = tree_node.get_type();
    bool const node_has_value = kv_pair.second.has_value();
    clp::ffi::Value node_value{};
    if (node_has_value) {
        node_value = kv_pair.second.value();
    }
    switch (ir_node_type) {
        case clp::ffi::SchemaTree::Node::Type::Int:
            return NodeType::Integer;
        case clp::ffi::SchemaTree::Node::Type::Float:
            return NodeType::Float;
        case clp::ffi::SchemaTree::Node::Type::Bool:
            return NodeType::Boolean;
        case clp::ffi::SchemaTree::Node::Type::UnstructuredArray:
            return NodeType::UnstructuredArray;
        case clp::ffi::SchemaTree::Node::Type::Str:
            if (node_value.is<std::string>()) {
                return NodeType::VarString;
            }
            return NodeType::ClpString;
        case clp::ffi::SchemaTree::Node::Type::Obj:
            if (node_has_value && node_value.is_null()) {
                return NodeType::NullValue;
            }
            return NodeType::Object;
        default:
            throw OperationFailed(ErrorCodeFailure, __FILENAME__, __LINE__);
    }
}

auto JsonParser::adjust_archive_node_type_for_timestamp(NodeType node_type, bool matches_timestamp)
        -> NodeType {
    if (false == matches_timestamp) {
        return node_type;
    }

    switch (node_type) {
        case NodeType::ClpString:
        case NodeType::VarString:
            return NodeType::DateString;
        default:
            return node_type;
    }
}

template <bool autogen>
auto JsonParser::add_node_to_archive_and_translations(
        uint32_t ir_node_id,
        clp::ffi::SchemaTree::Node const& ir_node_to_add,
        NodeType archive_node_type,
        int32_t parent_node_id,
        bool matches_timestamp
) -> int {
    auto const adjusted_archive_node_type
            = adjust_archive_node_type_for_timestamp(archive_node_type, matches_timestamp);

    auto key_name = ir_node_to_add.get_key_name();
    if (autogen && constants::cRootNodeId == parent_node_id) {
        // We adjust the name of the root of the auto-gen subtree to "@" in order to namespace the
        // auto-gen subtree within the archive's schema tree.
        key_name = constants::cAutogenNamespace;
    }
    int const curr_node_archive_id
            = m_archive_writer->add_node(parent_node_id, adjusted_archive_node_type, key_name);
    auto& ir_node_to_archive_node_id_mapping
            = autogen ? m_autogen_ir_node_to_archive_node_id_mapping
                      : m_ir_node_to_archive_node_id_mapping;
    ir_node_to_archive_node_id_mapping.emplace(
            std::make_pair(ir_node_id, archive_node_type),
            std::make_pair(curr_node_archive_id, matches_timestamp)
    );
    return curr_node_archive_id;
}

template <bool autogen>
auto JsonParser::get_archive_node_id_and_check_timestamp(
        uint32_t ir_node_id,
        NodeType archive_node_type,
        clp::ffi::SchemaTree const& ir_tree
) -> std::pair<int32_t, bool> {
    int curr_node_archive_id{constants::cRootNodeId};
    auto const& ir_node_to_archive_node_id_mapping
            = autogen ? m_autogen_ir_node_to_archive_node_id_mapping
                      : m_ir_node_to_archive_node_id_mapping;
    auto flat_map_location
            = ir_node_to_archive_node_id_mapping.find(std::pair{ir_node_id, archive_node_type});

    if (ir_node_to_archive_node_id_mapping.end() != flat_map_location) {
        return flat_map_location->second;
    }

    std::vector<uint32_t> ir_id_stack;
    ir_id_stack.push_back(ir_node_id);
    int32_t next_parent_archive_id{constants::cRootNodeId};
    NodeType next_node_type = archive_node_type;

    while (true) {
        auto const& curr_node = ir_tree.get_node(ir_id_stack.back());
        auto parent_of_curr_node_id = curr_node.get_parent_id();
        if (parent_of_curr_node_id.has_value()) {
            ir_id_stack.push_back(parent_of_curr_node_id.value());
            next_node_type = NodeType::Object;
        } else {
            next_parent_archive_id = constants::cRootNodeId;
            break;
        }

        flat_map_location = ir_node_to_archive_node_id_mapping.find(
                std::pair{ir_id_stack.back(), next_node_type}
        );
        if (ir_node_to_archive_node_id_mapping.end() != flat_map_location) {
            curr_node_archive_id = next_parent_archive_id = flat_map_location->second.first;
            ir_id_stack.pop_back();
            break;
        }
    }

    bool matches_timestamp{false};
    while (false == ir_id_stack.empty()) {
        auto const& curr_node = ir_tree.get_node(ir_id_stack.back());
        if (1 == ir_id_stack.size()) {
            matches_timestamp = m_archive_writer->matches_timestamp(
                    next_parent_archive_id,
                    curr_node.get_key_name()
            );
            curr_node_archive_id = add_node_to_archive_and_translations<autogen>(
                    ir_id_stack.back(),
                    curr_node,
                    archive_node_type,
                    next_parent_archive_id,
                    matches_timestamp
            );
        } else {
            curr_node_archive_id = add_node_to_archive_and_translations<autogen>(
                    ir_id_stack.back(),
                    curr_node,
                    NodeType::Object,
                    next_parent_archive_id,
                    false
            );
        }
        next_parent_archive_id = curr_node_archive_id;
        ir_id_stack.pop_back();
    }
    return {curr_node_archive_id, matches_timestamp};
}

template <bool autogen>
void JsonParser::parse_kv_log_event_subtree(
        KeyValuePairLogEvent::NodeIdValuePairs const& kv_pairs,
        clp::ffi::SchemaTree const& tree
) {
    for (auto const& pair : kv_pairs) {
        auto const archive_node_type = get_archive_node_type(tree, pair);
        auto const [node_id, matches_timestamp] = get_archive_node_id_and_check_timestamp<autogen>(
                pair.first,
                archive_node_type,
                tree
        );
        switch (archive_node_type) {
            case NodeType::Integer: {
                auto const i64_value
                        = pair.second.value().get_immutable_view<clp::ffi::value_int_t>();
                m_current_parsed_message.add_value(node_id, i64_value);
                if (matches_timestamp) {
                    m_archive_writer->ingest_timestamp_entry(m_timestamp_key, node_id, i64_value);
                }
            } break;
            case NodeType::Float: {
                auto const d_value
                        = pair.second.value().get_immutable_view<clp::ffi::value_float_t>();
                m_current_parsed_message.add_value(node_id, d_value);
                if (matches_timestamp) {
                    m_archive_writer->ingest_timestamp_entry(m_timestamp_key, node_id, d_value);
                }
            } break;
            case NodeType::Boolean: {
                auto const b_value
                        = pair.second.value().get_immutable_view<clp::ffi::value_bool_t>();
                m_current_parsed_message.add_value(node_id, b_value);
            } break;
            case NodeType::VarString: {
                auto const var_value{pair.second.value().get_immutable_view<std::string>()};
                if (matches_timestamp) {
                    uint64_t encoding_id{};
                    auto const timestamp = m_archive_writer->ingest_timestamp_entry(
                            m_timestamp_key,
                            node_id,
                            var_value,
                            encoding_id
                    );
                    m_current_parsed_message.add_value(node_id, encoding_id, timestamp);
                } else {
                    m_current_parsed_message.add_value(node_id, var_value);
                }
            } break;
            case NodeType::ClpString: {
                std::string decoded_value;
                if (pair.second.value().is<clp::ir::EightByteEncodedTextAst>()) {
                    decoded_value = pair.second.value()
                                            .get_immutable_view<clp::ir::EightByteEncodedTextAst>()
                                            .decode_and_unparse()
                                            .value();

                } else {
                    decoded_value = pair.second.value()
                                            .get_immutable_view<clp::ir::FourByteEncodedTextAst>()
                                            .decode_and_unparse()
                                            .value();
                }
                if (matches_timestamp) {
                    uint64_t encoding_id{};
                    auto const timestamp = m_archive_writer->ingest_timestamp_entry(
                            m_timestamp_key,
                            node_id,
                            decoded_value,
                            encoding_id
                    );
                    m_current_parsed_message.add_value(node_id, encoding_id, timestamp);
                } else {
                    m_current_parsed_message.add_value(node_id, decoded_value);
                }
            } break;
            case NodeType::UnstructuredArray: {
                std::string array_str;
                if (pair.second.value().is<clp::ir::EightByteEncodedTextAst>()) {
                    array_str = pair.second.value()
                                        .get_immutable_view<clp::ir::EightByteEncodedTextAst>()
                                        .decode_and_unparse()
                                        .value();
                } else {
                    array_str = pair.second.value()
                                        .get_immutable_view<clp::ir::FourByteEncodedTextAst>()
                                        .decode_and_unparse()
                                        .value();
                }
                m_current_parsed_message.add_value(node_id, array_str);
                break;
            }
            default:
                // Don't need to add value for obj or null
                break;
        }
        m_current_schema.insert_ordered(node_id);
    }
}

void JsonParser::parse_kv_log_event(KeyValuePairLogEvent const& kv) {
    clp::ffi::SchemaTree const& tree = kv.get_user_gen_keys_schema_tree();

    parse_kv_log_event_subtree<true>(
            kv.get_auto_gen_node_id_value_pairs(),
            kv.get_auto_gen_keys_schema_tree()
    );
    parse_kv_log_event_subtree<false>(
            kv.get_user_gen_node_id_value_pairs(),
            kv.get_user_gen_keys_schema_tree()
    );

    int32_t const current_schema_id = m_archive_writer->add_schema(m_current_schema);
    m_current_parsed_message.set_id(current_schema_id);
    m_archive_writer->append_message(current_schema_id, m_current_schema, m_current_parsed_message);
}

auto JsonParser::parse_from_ir() -> bool {
    constexpr size_t cDecompressorReadBufferCapacity{64 * 1024};  // 64 KB
    auto archive_creator_id = boost::uuids::to_string(m_generator());
    for (auto const& path : m_input_paths) {
        auto reader{try_create_reader(path, m_network_auth)};
        if (nullptr == reader) {
            std::ignore = m_archive_writer->close();
            return false;
        }

        clp::streaming_compression::zstd::Decompressor decompressor;
        size_t curr_pos{};
        size_t last_pos{};
        decompressor.open(*reader, cDecompressorReadBufferCapacity);

        auto deserializer_result{
                Deserializer<IrUnitHandler>::create(decompressor, IrUnitHandler{})
        };
        if (deserializer_result.has_error()) {
            auto err = deserializer_result.error();
            SPDLOG_ERROR(
                    "Encountered error while creating kv-ir deserializer: ({}) - {}",
                    err.value(),
                    err.message()
            );
            decompressor.close();
            check_and_log_curl_error(path, reader);
            std::ignore = m_archive_writer->close();
            return false;
        }
        auto& deserializer = deserializer_result.value();
        auto& ir_unit_handler{deserializer.get_ir_unit_handler()};

        size_t file_split_number{0ULL};
        int32_t log_event_idx_node_id{};
        auto initialize_fields_for_archive = [&]() -> bool {
            if (m_record_log_order) {
                log_event_idx_node_id
                        = add_metadata_field(constants::cLogEventIdxName, NodeType::DeltaInteger);
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cFilename},
                        path.path
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cFilename,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cFileSplitNumber},
                        file_split_number
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cFileSplitNumber,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            if (auto const rc = m_archive_writer->add_field_to_current_range(
                        std::string{constants::range_index::cArchiveCreatorId},
                        archive_creator_id
                );
                ErrorCodeSuccess != rc)
            {
                SPDLOG_ERROR(
                        "Failed to add metadata field \"{}\" ({})",
                        constants::range_index::cArchiveCreatorId,
                        static_cast<int64_t>(rc)
                );
                return false;
            }
            auto const& metadata = deserializer.get_metadata();
            if (metadata.contains(clp::ffi::ir_stream::cProtocol::Metadata::UserDefinedMetadataKey))
            {
                for (auto const& [metadata_key, metadata_value] :
                     metadata.at(clp::ffi::ir_stream::cProtocol::Metadata::UserDefinedMetadataKey)
                             .items())
                {
                    if (auto const rc = m_archive_writer->add_field_to_current_range(
                                metadata_key,
                                metadata_value
                        );
                        ErrorCodeSuccess != rc)
                    {
                        SPDLOG_ERROR(
                                "Failed to add metadata field \"{}\" ({})",
                                metadata_key,
                                static_cast<int64_t>(rc)
                        );
                        return false;
                    }
                }
            }
            return true;
        };
        if (false == initialize_fields_for_archive()) {
            decompressor.close();
            std::ignore = m_archive_writer->close();
            return false;
        }
        auto update_fields_after_archive_split = [&]() { ++file_split_number; };

        while (true) {
            auto const kv_log_event_result{deserializer.deserialize_next_ir_unit(decompressor)};

            if (kv_log_event_result.has_error()) {
                auto err = kv_log_event_result.error();
                SPDLOG_WARN(
                        "Encountered error while deserializing kv-ir log event from stream \"{}\": "
                        "({}) - {}",
                        path.path,
                        err.value(),
                        err.message()
                );
                if (check_and_log_curl_error(path, reader)) {
                    std::ignore = m_archive_writer->close();
                    decompressor.close();
                    return false;
                } else {
                    // Treat deserialization error as end of a truncated stream
                    break;
                }
            }
            if (kv_log_event_result.value() == clp::ffi::ir_stream::IrUnitType::EndOfStream) {
                break;
            }
            if (kv_log_event_result.value() == clp::ffi::ir_stream::IrUnitType::LogEvent) {
                auto const kv_log_event = &(ir_unit_handler.get_deserialized_log_event().value());

                m_current_schema.clear();

                // Add log_event_idx field to metadata for record
                if (m_record_log_order) {
                    m_current_parsed_message.add_value(
                            log_event_idx_node_id,
                            m_archive_writer->get_next_log_event_id()
                    );
                    m_current_schema.insert_ordered(log_event_idx_node_id);
                }

                try {
                    parse_kv_log_event(*kv_log_event);
                } catch (std::exception const& e) {
                    SPDLOG_ERROR("Encountered error while parsing a kv log event - {}", e.what());
                    std::ignore = m_archive_writer->close();
                    decompressor.close();
                    return false;
                }

                if (m_archive_writer->get_data_size() >= m_target_encoded_size) {
                    m_ir_node_to_archive_node_id_mapping.clear();
                    m_autogen_ir_node_to_archive_node_id_mapping.clear();
                    curr_pos = decompressor.get_pos();
                    m_archive_writer->increment_uncompressed_size(curr_pos - last_pos);
                    last_pos = curr_pos;
                    split_archive();
                    update_fields_after_archive_split();
                    if (false == initialize_fields_for_archive()) {
                        std::ignore = m_archive_writer->close();
                        decompressor.close();
                        return false;
                    }
                }

                ir_unit_handler.clear();
                m_current_parsed_message.clear();

            } else if (kv_log_event_result.value()
                       == clp::ffi::ir_stream::IrUnitType::SchemaTreeNodeInsertion)
            {
                continue;
            } else {
                SPDLOG_ERROR(
                        "Encountered unkown IR unit type ({}) during deserialization.",
                        static_cast<uint8_t>(kv_log_event_result.value())
                );
                std::ignore = m_archive_writer->close();
                decompressor.close();
                return false;
            }
        }
        m_ir_node_to_archive_node_id_mapping.clear();
        m_autogen_ir_node_to_archive_node_id_mapping.clear();
        curr_pos = decompressor.get_pos();
        m_archive_writer->increment_uncompressed_size(curr_pos - last_pos);
        decompressor.close();
        if (auto const rc = m_archive_writer->close_current_range(); ErrorCodeSuccess != rc) {
            SPDLOG_ERROR("Failed to close metadata range: {}", static_cast<int64_t>(rc));
            std::ignore = m_archive_writer->close();
            return false;
        }
    }
    return true;
}

auto JsonParser::store() -> std::vector<ArchiveStats> {
    m_archive_stats.emplace_back(m_archive_writer->close());
    return std::move(m_archive_stats);
}

void JsonParser::split_archive() {
    m_archive_stats.emplace_back(m_archive_writer->close(true));
    m_archive_options.id = m_generator();
    m_archive_writer->open(m_archive_options);
}

bool JsonParser::check_and_log_curl_error(
        Path const& path,
        std::shared_ptr<clp::ReaderInterface> reader
) {
    if (auto network_reader = std::dynamic_pointer_cast<clp::NetworkReader>(reader);
        nullptr != network_reader)
    {
        if (auto const rc = network_reader->get_curl_ret_code();
            rc.has_value() && CURLcode::CURLE_OK != rc.value())
        {
            auto const curl_error_message = network_reader->get_curl_error_msg();
            SPDLOG_ERROR(
                    "Encountered curl error while ingesting {} - Code: {} - Message: {}",
                    path.path,
                    static_cast<int64_t>(rc.value()),
                    curl_error_message.value_or("Unknown error")
            );
            return true;
        }
    }
    return false;
}
}  // namespace clp_s
