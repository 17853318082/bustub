//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  // 向叶子节点页面中插入数据（key，value）
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &key_comparator) -> int;
  // 获取节点所在的位置
  auto KeyIndex(const KeyType &key, const KeyComparator &key_comparator) -> int;

  // 根据index获取键值对
  auto GetItem(const int index) -> MappingType;
  // 根据key，获取节点value
  auto Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) -> bool;

  //  将该节点的一般数据 移动到另一个节点
  void MoveHalfTo(BPlusTreeLeafPage *recipient);

  // 从另一个节点中接收N个数据
  void ReceiveN(MappingType *item, int size);

  // 从叶节点中删除指定数据
  auto RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int;

  // 将节点从前到后复制到当前节点
  void MoveFirstToLast(BPlusTreeLeafPage *node);

  // 将指定数据插入到节点的最后一个
  void InsertLast(const MappingType &item);

  // 将节点从前到后复制到当前节点
  void MoveLastToFirst(BPlusTreeLeafPage *node);

  // 将指定数据插入到节点的第一个
  void InsertFirst(const MappingType &item);

  // 将指定所有数据移动到指定节点
  void MoveAllTo(BPlusTreeLeafPage *node);

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
