cmake_minimum_required(VERSION 3.9)

set(CMAKE_CXX_STANDARD 11)

project(leveldb_study)

aux_source_directory(src SRC_LIST)

add_subdirectory(thrid_party/leveldb)

add_executable(leveldb_study ${SRC_LIST})

target_link_libraries(leveldb_study leveldb)
