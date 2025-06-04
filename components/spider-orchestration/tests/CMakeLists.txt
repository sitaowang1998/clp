
set(ORCHESTRATION_TEST_SOURCES
    compression/test-compression_task.cpp
    CACHE INTERNAL
    "Orchestration test sources"
)

add_executable(unitTest)
target_sources(unitTest PRIVATE ${ORCHESTRATION_TEST_SOURCES})
target_include_directories(unitTest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(unitTest PRIVATE
    compression_task
)
target_link_libraries(unitTest PRIVATE Catch2::Catch2WithMain)
