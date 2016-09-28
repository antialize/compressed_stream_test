cmake_minimum_required(VERSION 2.8)
project(tpie)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/")

find_package(Snappy)
include_directories(${Snappy_INCLUDE_DIR} .)

add_library(stream STATIC file_stream.h file_stream.cpp)
target_link_libraries(stream ${Snappy_LIBRARY})

add_executable(t test.cpp)
target_link_libraries(t stream)

