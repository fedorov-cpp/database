cmake_minimum_required(VERSION 3.17)
project(database)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MACOSX_RPATH OFF)

include(cmake/compile_flags.cmake)

# Conan package manager
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

# Boost
find_package(Boost 1.71 REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

# Local libs
include_directories(${PROJECT_SOURCE_DIR})

add_library(database SHARED database.cpp)

add_library(Server SHARED Server.cpp)
target_link_libraries(Server database)

add_library(Client SHARED Client.cpp)
target_link_libraries(Client database)

# Local executables
add_executable(server_runner server_runner.cpp)
target_link_libraries(server_runner Server boost_program_options boost_filesystem)

add_executable(client_runner client_runner.cpp)
target_link_libraries(client_runner Client boost_program_options)