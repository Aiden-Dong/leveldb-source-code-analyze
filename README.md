#### Google LevelDB 源码阅读

基于[1.20版本](https://github.com/google/leveldb/tree/v1.20
)
---
当前绝大多数的KV数据库都是基于LevelDB的设计思想:类似于RockesDB,HBase等。

为了更加细致的了解基于LSM Tree的KV数据库的设计细节，所以花了些时间来阅读源码，并共享出来希望和大家一起探讨

代码部分已经注释说明,并且分为以下几个专题整理 : 

- [SSTable 文件格式](https://aiden-dong.github.io/2022/03/11/Leveldb%E4%B9%8BSSTable%E6%95%B0%E6%8D%AE%E6%A0%BC%E5%BC%8F/)
- [Leveldb之数据写入过程](https://aiden-dong.github.io/2022/05/09/Leveldb%E4%B9%8B%E6%95%B0%E6%8D%AE%E5%86%99%E5%85%A5%E8%BF%87%E7%A8%8B/)
- [leveldb数据读取过程](https://aiden-dong.github.io/2022/05/11/leveldb%E6%95%B0%E6%8D%AE%E8%AF%BB%E5%8F%96%E7%AF%87/)
- [Leveldb之MVCC实现](https://aiden-dong.github.io/2022/05/19/Leveldb%E4%B9%8BMVCC%E5%AE%9E%E7%8E%B0/)

欢迎访问，并给出意见
