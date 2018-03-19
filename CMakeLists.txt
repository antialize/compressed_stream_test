cmake_minimum_required(VERSION 2.8)
project(compressed_stream_test)

include(${CMAKE_CURRENT_SOURCE_DIR}/default_flags.cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules/")

add_definitions("-Wall -Wextra")

find_package(Snappy)
find_package(Threads REQUIRED)
include_directories(${Snappy_INCLUDE_DIR} .)

find_package(Boost COMPONENTS filesystem system REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


add_library(stream STATIC file_stream.h available_blocks.cpp stream.cpp file.cpp job.cpp misc.cpp file_utils.cpp exception.h log.h file_stream_impl.h tpie/is_simple_iterator.h tpie/serialization2.h defaults.h)
target_link_libraries(stream ${Snappy_LIBRARY} ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

add_executable(t test.cpp check_file.cpp check_file.h)
target_link_libraries(t stream)

add_executable(random_test random_test.cpp)
target_link_libraries(random_test stream)

add_executable(speed_test speed_test.cpp speed_test_common.h)
target_link_libraries(speed_test stream)

add_executable(dump_file dump_file.cpp check_file.cpp check_file.h)
target_link_libraries(dump_file stream)
