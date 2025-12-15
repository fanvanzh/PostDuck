cmake_minimum_required(VERSION 3.5...3.29)
project(postduck LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_definitions(-DBOOST_LOG_DYN_LINK=1)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

find_package(Boost 1.65 REQUIRED COMPONENTS program_options log log_setup date_time thread system filesystem)
add_subdirectory(duckdb)

include_directories(${Boost_INCLUDE_DIRS} include duckdb/src/include)

file(GLOB SOURCES "src/*.cpp") 
add_executable(postduck ${SOURCES})
target_link_libraries(postduck PUBLIC
 duckdb
 boost_log_setup boost_log boost_program_options boost_filesystem boost_thread boost_system pthread)
