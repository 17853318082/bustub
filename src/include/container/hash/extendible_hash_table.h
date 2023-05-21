//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  /* explicit：当使用该关键字声明构造时，表示防止在程序中进行隐式类型转换 */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.   获得目录的全局深度
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * 给定一个目录索引，获取它所指向的桶的本地深度。
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.  获取目录中桶的数量
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key. 查找与给定key相关联的value
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched. 要搜索的key
   * @param[out] value The value associated with the key.  与key相关联的value，value传入传出参数
   * @return True if the key is found, false otherwise.  返回是否存在这个key
   */
  /* 当派生类继承自基类并重新定义一个虚函数时，可以使用override关键字显式地声明该函数是对基类虚函数的覆盖 */
  auto Find(const K &key, V &value) -> bool override;

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
  void Insert(const K &key, const V &value) override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table. 给定一个key移除hash表中的键值对
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.     key为要删除的key
   * @return True if the key exists, false otherwise. 如果存在则返回true 否则返回false
   */
  auto Remove(const K &key) -> bool override;

  /**
   * Bucket class for each hash table bucket that the directory points to.  每个哈希表桶的Bucket类，目录指向该类。
   * 在哈希表中，桶是用于存储数据的容器，每个桶包含一个或多个键
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full.检测桶是否充满 */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket.检测本地bucket深度 */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket.增加本地bucket深度 */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Find the value associated with the given key in the bucket. 在桶中查找与给定键相关联的值。
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket. 在桶中删除与给定键相关联的键值对。
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

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
    auto Insert(const K &key, const V &value) -> bool;

   private:
    // TODO(student): You may add additional private members and helper functions
    size_t size_;                      // 桶的最大长度
    int depth_;                        // 桶深度，桶中元素的个数
    std::list<std::pair<K, V>> list_;  // bucket,使用list
  };

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;                          // The global depth of the directory 目录的全局深度
  size_t bucket_size_;                        // The size of a bucket bucket的长度
  int num_buckets_;                           // The number of buckets in the hash table bucket的数量
  mutable std::mutex latch_;                  // 读写锁
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table  hash表的目录，使用vector

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
