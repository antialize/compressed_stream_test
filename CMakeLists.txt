cmake_minimum_required(VERSION 2.8)
project(tpie)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/")

find_package(Snappy)
find_package(Threads REQUIRED)
include_directories(${Snappy_INCLUDE_DIR} .)

add_library(stream STATIC file_stream.h file_stream.cpp available_blocks.cpp file.cpp job.cpp misc.cpp)
target_link_libraries(stream ${Snappy_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})

add_executable(t test.cpp)
target_link_libraries(t stream)

