#include "compression.hpp"

#include <string>
#include <unistd.h>
#include <vector>

#include <nlohmann/json.hpp>

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
    // Create an input and output pipe to communicate with the compression process
    pid_t input_pipe[2];
    auto input_pipe_result = pipe(input_pipe);
    if (input_pipe_result == -1) {
        return "Failed to create input pipe";
    }
    pid_t output_pipe[2];
    auto output_pipe_result = pipe(output_pipe);
    if (output_pipe_result == -1) {
        close(input_pipe[0]);
        close(input_pipe[1]);
        return "Failed to create output pipe";
    }

    auto pid = fork();
    if (pid < 0) {
        close(input_pipe[0]);
        close(input_pipe[1]);
        close(output_pipe[0]);
        close(output_pipe[1]);
        return "Failed to fork process";
    }
    if (pid == 0) {
        // Child process
        auto args = {
            "--input-pipe-read", std::to_string(input_pipe[0]),
            "--input-pipe-write", std::to_string(input_pipe[1]),
            "--output-pipe-read", std::to_string(output_pipe[0]),
            "--output-pipe-write", std::to_string(output_pipe[1])
        };
        auto args_cstr = std::array<char const*, 6>;
        for (size_t i = 0; i < args_cstr.size(); ++i) {
            args_cstr[i] = args[i].c_str();
        }

        execvp(
            clp_compression_task_path.c_str(),
            const_cast<char *const *>(args_cstr.data())
        );

        // If execvp fails, exit with an error code
        _exit(1);
    }
    // Parent process
    close(input_pipe[0]);
    close(output_pipe[1]);

    nlohmann::json input_json = {
        {"job_id", job_id},
        {"task_id", task_id},
        {"tag_ids", tag_ids},
        {"clp_io_config_json", clp_io_config_json},
        {"paths_to_compression_json", paths_to_compression_json},
        {"clp_metadata_db_connection_config", clp_metadata_db_connection_config},
        {"clp_compression_task_path", clp_compression_task_path}
    };

    std::string input_str = input_json.dump();
    ssize_t bytes_written = write(input_pipe[1], input_str.c_str(), input_str.size());
    if (bytes_written == -1) {
        close(input_pipe[1]);
        close(output_pipe[0]);
        return "Failed to write to input pipe";
    }
    if (bytes_written < static_cast<ssize_t>(input_str.size())) {
        close(input_pipe[1]);
        close(output_pipe[0]);
        return "Partial write to input pipe";
    }
    std::string output_str;
    char buffer[4096];
    ssize_t bytes_read;
    while ((bytes_read = read(output_pipe[0], buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytes_read] = '\0';  // Null-terminate the string
        output_str += buffer;
    }

    close(input_pipe[1]);
    close(output_pipe[0]);
    return output_str;
}

}  // namespace orchestration::compression
