#include "compression.hpp"

#include <string>

namespace orchestration::compression {
auto compress(
    int job_id,
    int task_id,
    std::vector<int> const &tag_ids,
    std::string const &clp_io_config_json,
    std::string const &paths_to_compression_json,
    std::string const &clp_metadata_db_connection_config,
    std::string const &clp_compression_task_path
) -> std::string {
    return "";
}

}  // namespace orchestration::compression
