cmake_minimum_required(VERSION 3.7)
project(table_storage)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        main.cpp
        slice.h
        status.h
        random.h
        table.h
        table.cpp
        writable_file.h
        writable_file.cpp
        coding.h
        coding.cpp
        random_access_file.h
        random_access_file.cpp)

add_executable(table_storage ${SOURCE_FILES})
target_link_libraries(table_storage gtest pthread)