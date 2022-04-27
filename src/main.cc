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

// 测试内存对齐
void test_mem_align();

// 测试大端与小端字节
void test_bytes();


// 测试二分查找法
void test_search();


int main(int argc, char*argv[]){

  test_leveldb_normal();

  return 0;
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

void test_mem_align(){
  std::cout << sizeof(void *) << std::endl;
  std::cout << sizeof(int) <<  std::endl;
  std::cout << sizeof(short) << std::endl;


  struct node_struct{
    int a;
    short b;
  };

  std::cout << sizeof(node_struct) << std::endl;
}

void test_bytes(){
  // 数组往高位生长
  int8_t arr[8] {0,0,0,0,0,0,0,1};
  uint32_t a(1);


  for (int i = 0; i < 8; ++i) {
    std::cout << static_cast<int>(arr[i])  << ":" << static_cast<const void *>(&arr[i]) << std::endl;
  }
  /***
   * 0:0x7ffee9106670
   * 0:0x7ffee9106671
   * 0:0x7ffee9106672
   * 0:0x7ffee9106673
   * 0:0x7ffee9106674
   * 0:0x7ffee9106675
   * 0:0x7ffee9106676
   * 1:0x7ffee9106677
   */

  uint64_t *p(reinterpret_cast<uint64_t *>(arr));

  std::cout << std::hex << *p << std::endl;
  // 输出 : 01 00 00 00 00 00 00 00

  std::cout << std::endl;
}

void test_search(){
  u_int16_t arr[10] = {1, 3, 7, 8 , 10, 12, 14, 15, 16, 19};

  u_int16_t search_var = 13;


  u_int16_t v_left = 0;
  u_int16_t v_right = 9;
  u_int16_t v_middle = 0;

  while (v_left < v_right){
    v_middle = (v_left  + v_right) / 2;

    if(arr[v_middle] < search_var) v_left = v_middle + 1;
    else v_right = v_middle;
  }

  std::cout << std::dec << arr[v_right] << std::endl;
}