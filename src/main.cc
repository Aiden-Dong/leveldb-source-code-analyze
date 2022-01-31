#include "leveldb/db.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "leveldb/write_batch.h"
#include <iostream>
#include <string>

typedef uint64_t Key;
struct Comparator {
    int operator()(const Key& a, const Key& b) const {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return +1;
        } else {
            return 0;
        }
    }
};

// 测试 leveldb 的普通读写流程
void test_leveldb_normal();

// 测试定位 new 运算符
void test_new_operator();

// 测试跳表
void test_skip_list();

int main(int argc, char*argv[]){
    test_skip_list();
}

void test_leveldb_normal(){
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
}

void test_new_operator(){
    char arr[200];

    double *p1 = new(arr)double{12.3};

    std::cout << sizeof(double) << ":" << *p1 << std::endl;

    long *p2 = new(arr + sizeof(double))long{12};

    std::cout << sizeof(long) << ":" << *p2 << std::endl;

    std::cout << *p1 << std::endl;
}

void test_skip_list(){
    leveldb::Arena arena;
    Comparator cmp;
    leveldb::SkipList<Key, Comparator> list(cmp, &arena);

    list.Insert(200);
    list.Insert(100);
    list.Insert(150);
    list.Insert(20);
    list.Insert(18);
    list.Insert(289);
    list.Insert(152);
    list.Insert(94);


}