
set(ORCHESTRATION_TEST_SOURCES
    compression/test-compression_task.cpp
    CACHE INTERNAL
    "Orchestration test sources"
)

add_executable(spider-orchestration-test)
target_sources(spider-orchestration-test PRIVATE ${ORCHESTRATION_TEST_SOURCES})
target_include_directories(spider-orchestration-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(spider-orchestration-test PRIVATE
    compression_task
)
target_link_libraries(spider-orchestration-test PRIVATE Catch2::Catch2WithMain)
