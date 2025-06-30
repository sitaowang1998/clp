set(COMPRESSION_TASK_SOURCES
    compression/compression.cpp
    CACHE INTERNAL
    "Source files for compression task"
)

set(COMPRESSION_TASK_HEADERS
    compression/compression.hpp
    CACHE INTERNAL
    "Header files for compression task"
)

add_library(compression_task)
target_sources(compression_task PRIVATE ${COMPRESSION_TASK_SOURCES})
target_sources(compression_task PUBLIC ${COMPRESSION_TASK_HEADERS})
target_include_directories(
    compression_task
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
target_link_libraries(compression_task PUBLIC spider::spider)
