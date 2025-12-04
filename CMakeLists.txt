cmake_minimum_required(VERSION 3.12)
project(eloqdb C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 99)

# Options to select compute engines
option(WITH_ELOQKV "Build with EloqKV engine" ON)
option(WITH_ELOQSQL "Build with EloqSQL engine" ON)

if(NOT WITH_ELOQKV AND NOT WITH_ELOQSQL)
    message(FATAL_ERROR "At least one compute engine must be enabled")
endif()

message(STATUS "Building EloqDB with:")
message(STATUS "  - EloqKV: ${WITH_ELOQKV}")
message(STATUS "  - EloqSQL: ${WITH_ELOQSQL}")

# Set compile definitions for enabled engines
if(WITH_ELOQKV)
    add_compile_definitions(ELOQ_MODULE_ELOQKV)
endif()

if(WITH_ELOQSQL)
    add_compile_definitions(ELOQ_MODULE_ELOQSQL)
endif()

# Build order:
# 1. Build data_substrate first (shared by all engines)
message(STATUS "Building data_substrate...")
add_subdirectory(data_substrate)

# 2. Build eloqkv as library if enabled
if(WITH_ELOQKV)
    message(STATUS "Building eloqkv as library...")
    set(BUILD_ELOQKV_AS_LIBRARY ON CACHE BOOL "Build eloqkv as library" FORCE)
    add_subdirectory(eloqkv)
endif()

# 3. Build eloqsql as library if enabled
if(WITH_ELOQSQL)
    message(STATUS "Building eloqsql as library...")
    set(BUILD_ELOQSQL_AS_LIBRARY ON CACHE BOOL "Build eloqsql as library" FORCE)
    add_subdirectory(eloqsql)
endif()

# 4. Build EloqDB binary
# Include necessary dependencies
include(FindThreads)
include(FindProtobuf)

# Find required libraries
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)

find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
find_library(GLOG_LIB NAMES glog)

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES brpc)

option(BRPC_WITH_GLOG "With glog" ON)
message(NOTICE "BRPC_WITH_GLOG : ${BRPC_WITH_GLOG}")
include_directories(
    ${GFLAGS_INCLUDE_PATH}
    ${GLOG_INCLUDE_PATH}
    ${BRPC_INCLUDE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Collect all libraries needed for linking
set(ELOQDB_LIBS
    data_substrate
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${GLOG_LIB}
)

# Add engine-specific libraries
if(WITH_ELOQKV)
    list(APPEND ELOQDB_LIBS ${ELOQKV_LIBRARY})
    include_directories(${ELOQKV_INCLUDE_DIRS})
    list(APPEND ELOQDB_LIBS ${BRPC_LIB})
endif()

if(WITH_ELOQSQL)
    list(APPEND ELOQDB_LIBS 
        ${ELOQSQL_SQL_LIBRARY}
        ${ELOQSQL_SQL_BUILTINS_LIBRARY}
    )
    include_directories(${ELOQSQL_INCLUDE_DIRS})
endif()

# Create EloqDB executable
add_executable(eloqdb src/main.cpp)

target_link_libraries(eloqdb ${ELOQDB_LIBS})

# Set runtime paths
set_target_properties(eloqdb PROPERTIES
    BUILD_RPATH "$ORIGIN/../lib"
    INSTALL_RPATH "$ORIGIN/../lib"
    INSTALL_RPATH_USE_LINK_PATH TRUE
)

# Installation
install(TARGETS eloqdb RUNTIME DESTINATION bin)

