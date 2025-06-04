
set(COMPRESSION_CLIENT_SOURCES
        compression/client.cpp
    CACHE INTERNAL
    "Compression client source files"
)

add_executable(compression_client)
target_sources(compression_client PRIVATE ${COMPRESSION_CLIENT_SOURCES})
target_include_directories(compression_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../..)
