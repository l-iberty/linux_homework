cmake_minimum_required(VERSION 3.7)
project(table_storage)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        main.cpp
        slice.h
        status.h
        random.h
        posix_env.h
        table.h
        table.cpp
        coding.h
        coding.cpp
        env.h
        env.cpp mutexlock.h)

add_executable(table_storage ${SOURCE_FILES})
target_link_libraries(table_storage gtest pthread)