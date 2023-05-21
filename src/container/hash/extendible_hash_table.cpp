//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

/** 构造函数：初始化 */
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 对目录进行初始化-- 创建一个桶大小为bucket_size ，并放入目录
  this->dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

/** 根据key查找桶索引 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

/** 获得全局深度*/
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

/** 查看局部深度*/
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

/** 返回局部分度*/
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // 加锁
  std::unique_lock<std::mutex> lock(this->latch_);
  // 寻找所在的桶
  auto index = IndexOf(key);
  auto target_bucket = this->dir_[index];
  // 执行桶查找
  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // 加锁
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 获得寻找所在的桶
  auto index = IndexOf(key);
  auto target_bucket = this->dir_[index];
  // 执行 删除
  return target_bucket->Remove(key);
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the hash table.  给定一个键值对插入到hash表中
 * If a key already exists, the value should be updated.        如果key已经存在，则value应该被更新
 * If the bucket is full and can't be inserted, do the following steps before retrying:
 * 如果buket已经满了则应该进行以下操作
 *    1. If the local depth of the bucket is equal to the global depth,
 * 如果本地桶的数量等于全局深度则增加全局深度，长度增加两倍 increment the global depth and double the size of the
 * directory.
 *    2. Increment the local depth of the bucket.          增加本地bucket深度
 *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
 * 将桶拆分，并重新分配目录指针和桶中的键值对。
 *
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // 加锁 -- 自动解锁，使用RAII机制，在作用域结束时自动释放锁
  std::scoped_lock<std::mutex> lock(this->latch_);
  // 如果对应的桶已经满了，则进行扩充，有可能存在多个桶已经满的情况，所以使用循环扩充
  while (this->dir_[IndexOf(key)]->IsFull()) {
    // std::cout<<"目录扩容+桶分裂"<<std::endl;
    auto index = IndexOf(key);               // 获取桶索引
    auto target_bucket = this->dir_[index];  // 查找对应桶

    // 桶深度==全局深度-->目录扩充
    if (target_bucket->GetDepth() == this->GetGlobalDepthInternal()) {
      // 全局深度+1
      this->global_depth_++;
      int capacity = this->dir_.size();
      // 长度增加两倍,后面桶值等于前面的桶
      this->dir_.resize(capacity << 1);

      for (int i = 0; i < capacity; i++) {
        this->dir_[i + capacity] = this->dir_[i];
      }
    }

    // 以进行完目录扩充或者当前桶已满-->桶分裂 -- 将原先的桶分裂为两个桶,桶深度*2
    int mask = 1 << target_bucket->GetDepth();
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);

    for (const auto &it : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(it.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(it.first, it.second);
      } else {
        bucket_0->Insert(it.first, it.second);
      }
    }
    // 桶数量+1 -- 正在使用的桶
    this->num_buckets_++;
    // 将桶放入两个不同索引中
    for (size_t i = 0; i < this->dir_.size(); i++) {
      if (this->dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }
  // 如果桶未满
  auto index = IndexOf(key);
  auto target_bucket = this->dir_[index];
  // 执行插入
  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Find the value associated with the given key in the bucket. 在桶中查找与给定键相关联的值。
 * @param key The key to be searched.
 * @param[out] value The value associated with the key.
 * @return True if the key is found, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // 如果bucket未空，则返回false
  if (this->list_.empty()) {
    return false;
  }
  // 不为空时开始查找
  auto it = this->list_.begin();
  while (it != this->list_.end()) {
    // 找到了该值，返回true
    if (it->first == key) {
      value = it->second;
      return true;
    }
    it++;
  }
  // 未找到，返回false
  return false;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Given the key, remove the corresponding key-value pair in the bucket. 在桶中删除与给定键相关联的键值对。
 * @param key The key to be deleted.
 * @return True if the key exists, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // 如果桶为空，则返回false
  if (this->list_.empty()) {
    return false;
  }
  // 当桶未空时，查找，对其进行删除
  for (auto it = this->list_.begin(); it != this->list_.end(); it++) {
    // 找到了这个key则对其进行删除，并返回true
    if (it->first == key) {
      this->list_.erase(it);
      return true;
    }
  }
  // 未找到，则返回false
  return false;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the bucket.  将给定的键值对插入到桶中。
 *      1. If a key already exists, the value should be updated.  如果key已经存在则应该更新value
 *      2. If the bucket is full, do nothing and return false.   如果bucket已经充满，则返回false
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 * @return True if the key-value pair is inserted, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 查看bucket是否已满，如果已满则返回false
  if (this->IsFull()) {
    return false;
  }
  // 如果bucket的未满，则进行插入操作
  for (auto v : this->list_) {
    // 如果key已经存在于bucket则更新value,并返回true
    if (v.first == key) {
      v.second = value;
      return true;
    }
  }
  // 此时表明key不在bucket中，则插入key，value，在尾部插入
  this->list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
