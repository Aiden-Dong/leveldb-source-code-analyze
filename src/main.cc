#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <iostream>
#include <string>

int main(){

    leveldb::DB * ldbptr = nullptr;
    leveldb::Options options;

    options.create_if_missing = true;

    const std::string db_path = "./testdb.ldb";
    const std::string key = "testkey";
    const std::string value = "hello leveldb";

    leveldb::Status status = leveldb::DB::Open(options, db_path, &ldbptr);

    if(!status.ok()){
        std::cerr << "open leveldb error" << std::endl;
        exit(255);
    }else if(ldbptr == nullptr){
        std::cerr << "open leveldb error" << std::endl;
        exit(255);
    }

    std::cout << "open leveldb success" << std::endl;

    // 写入数据
    status = ldbptr->Put(leveldb::WriteOptions(), key, value);

    if(!status.ok()){
        std::cerr << "write data error" << std::endl;
    }

    std::cout << "write data success" << std::endl;

    // 读取数据
    std::string readvalue;
    status = ldbptr->Get(leveldb::ReadOptions(), key, &readvalue);

    if(!status.ok()){
        std::cerr << "read data error" << std::endl;
    }


    std::cout << "read data success : " << readvalue << std::endl;


    return 0;
}