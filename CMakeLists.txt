cmake_minimum_required(VERSION 3.9)

set(CMAKE_CXX_STANDARD 11)

project(leveldb_study)

aux_source_directory(src SRC_LIST)

#aux_source_directory(thrid_party/leveldb/db LEVELDB_DB)

add_subdirectory("thrid_party/leveldb")


include_directories("${CMAKE_SOURCE_DIR}/thrid_party/leveldb")
#include_directories("${CMAKE_SOURCE_DIR}/thrid_party/leveldb/port")

add_executable(leveldb_study ${SRC_LIST} ${LEVELDB_DB} thrid_party/leveldb/db/log_reader.cc)


enable_testing()
# leveldb
target_link_libraries(leveldb_study leveldb)

# googletest
target_link_libraries(leveldb_study gtest_main)
