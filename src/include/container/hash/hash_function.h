//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_function.h
//
// Identification: src/include/container/hash/hash_function.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "murmur3/MurmurHash3.h"

namespace bustub {

/* 作用：将key转换为响应的hash值，用来寻找桶的位置 */
template <typename KeyType>
class HashFunction {
 public:
  /**
   * @param key the key to be hashed 输入hash表的key
   * @return the hashed value  返回hash的value
   */
  virtual auto GetHash(KeyType key) -> uint64_t {
    uint64_t hash[2];
    murmur3::MurmurHash3_x64_128(reinterpret_cast<const void *>(&key), static_cast<int>(sizeof(KeyType)), 0,
                                 reinterpret_cast<void *>(&hash));
    return hash[0];
  }
};

}  // namespace bustub
