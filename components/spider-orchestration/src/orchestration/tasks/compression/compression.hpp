#ifndef ORCHESTRATION_TASKS_COMPRESSION_COMPRESSION_HPP
#define ORCHESTRATION_TASKS_COMPRESSION_COMPRESSION_HPP

#include <string>

namespace orchestration::compression {
auto compress(std::string const& input) -> std::string;
}  // namespace orchestration::compression

#endif
