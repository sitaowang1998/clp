#ifndef ORCHESTRATION_TASKS_COMPRESSION_COMPRESSION_HPP
#define ORCHESTRATION_TASKS_COMPRESSION_COMPRESSION_HPP

#include <string>
#include <vector>

#include <spider/client/spider.hpp>

namespace orchestration::compression {
auto clp_compress(
    spider::TaskContext& context,
    int job_id,
    int task_id,
    std::vector<int> const &tag_ids,
    std::string const &clp_io_config_json,
    std::string const &paths_to_compression_json,
    std::string const &clp_metadata_db_connection_config,
    std::string const &clp_compression_task_path
) -> std::string;
} // namespace orchestration::compression

#endif
