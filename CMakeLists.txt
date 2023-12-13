cmake_minimum_required(VERSION 3.26)
project(lock_free_eht)

set(CMAKE_CXX_STANDARD 17)

add_library(myLibrary STATIC include/eht.h
        include/coarse-eth.h
        include/hash_function.h
        lib/murmur3/MurmurHash3.cpp
        lib/comparator/int-comparator.h
        lib/node/node.cpp
        lib/node/node.h
        lib/node/inner-node.hpp
        lib/node/leaf-node.hpp
        include/eth_storage/htable_header.h
        include/eth_storage/htable_directory.h
        include/common.h
        include/eth_storage/htable_bucket.h)


target_include_directories(myLibrary PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})


add_executable(lock_free_eht
        main.cpp
)
target_link_libraries(lock_free_eht myLibrary)

