cmake_minimum_required(VERSION 3.16.3)

project(spider-orchestration LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "Enable/Disable output of compile commands during generation." FORCE)

# Set the default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(ORCHESTRATION_DEFAULT_BUILD_TYPE "Release")
    message(STATUS "No build type specified. Setting to '${ORCHESTRATION_DEFAULT_BUILD_TYPE}'.")
    set(CMAKE_BUILD_TYPE
            "${ORCHESTRATION_DEFAULT_BUILD_TYPE}"
            CACHE STRING
            "Choose the type of build."
            FORCE
    )
endif()

# Add local CMake module directory to CMake's modules path
set(CMAKE_MODULE_PATH
        ${CMAKE_MODULE_PATH}
        "${PROJECT_SOURCE_DIR}/cmake/Modules/"
)

# Macro providing the length of the absolute source directory path so we can
# create a relative (rather than absolute) __FILE__ macro
string(LENGTH "${PROJECT_SOURCE_DIR}/" SOURCE_PATH_SIZE)
add_definitions("-DSOURCE_PATH_SIZE=${SOURCE_PATH_SIZE}")

# Profiling options
add_definitions(-DPROF_ENABLED=0)

# Compile-in debug logging statements
#add_definitions(-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

# Flush to disk switch
add_definitions(-DFLUSH_TO_DISK_ENABLED=1)

# Make off_t 64-bit
add_definitions(-D_FILE_OFFSET_BITS=64)

# Turn on PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Detect linking mode (static or shared); Default to static
set(ORCHESTRATION_USE_STATIC_LIBS ON CACHE BOOL "Whether to link against static libraries.")
if (ORCHESTRATION_USE_STATIC_LIBS)
    if (APPLE)
        set (ORCHESTRATION_STATIC_LOBS_UNSUPPORTED_PLATFORM "macOS")
    elseif (EXISTS "/etc/centOS-release")
        set (ORCHESTRATION_STATIC_LOBS_UNSUPPORTED_PLATFORM "CentOS")
    endif()

    if (DEFINED ORCHESTRATION_STATIC_LOBS_UNSUPPORTED_PLATFORM)

        set(ORCHESTRATION_USE_STATIC_LIBS OFF)
    endif()
endif()

if (ORCHESTRATION_USE_STATIC_LIBS)
    set(ORCHESTRATION_LIBS_STRING "static")
else()
    set(ORCHESTRATION_LIBS_STRING "shared")
endif()
message(STATUS "Building using ${ORCHESTRATION_LIBS_STRING} libraries.")

find_package(Catch2 3.8.0 REQUIRED)
if(Catch2_FOUND)
    message(STATUS "Found Catch2 ${Catch2_VERSION}.")
else()
    message(FATAL_ERROR "Could not find libraries for Catch2.")
endif()

add_subdirectory(src/orchestration)

add_subdirectory(tests)
