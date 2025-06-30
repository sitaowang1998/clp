add_custom_target(compress_client)
add_custom_command(
    TARGET compress_client
    PRE_BUILD
    COMMAND
        ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/compression
        ${CMAKE_CURRENT_BINARY_DIR}/client/compression
)
