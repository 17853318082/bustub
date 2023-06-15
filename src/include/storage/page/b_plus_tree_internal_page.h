//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  // 根据index查找value
  auto ValueAt(int index) const -> ValueType;
  // 根据value查找index
  auto IndexAt(const ValueType &value) -> int;
  // 将在index位置的值设置为value
  void SetValueAt(int index, const ValueType &value);
  auto Lookup(const KeyType &key, const KeyComparator &keyComparator) const -> ValueType;

  //  移动一半的数据 ，到另一个节点上
  void MoveHalfTo(BPlusTreeInternalPage *pointer, BufferPoolManager *buffer_pool_manager);

  // 接收N个数据
  void ReceiveN(MappingType *item, int size, BufferPoolManager *buffer_pool_manager);
  // 新建一个根节点，将新节点和就节点，加入到新结点中
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  auto InsertNodeAfter(const ValueType &old_node, const KeyType &key, const ValueType &new_node) -> int;

  // 从兄弟节点的前面拿一个值，放到当前节点的后面,将两个节点的中间节点，放到当前节点的第一个
  void MoveFirstToLast(BPlusTreeInternalPage* node,const KeyType& middle_key,BufferPoolManager* buffer_pool_manager);
  // 将指定数据添加到节点的最后一个位置
  void InsertLast(const MappingType &item,BufferPoolManager* buffer_pool_manager);

  // 从兄弟的节点的后面拿一个值，放到当前节点的前面
  void MoveLastToFirst(BPlusTreeInternalPage* node,const KeyType& middle_key,BufferPoolManager* buffer_pool_manager);
  void InsertFirst(const MappingType &item,BufferPoolManager* buffer_pool_manager);

  // 将该节点的所有数据移动到另一个节点
  void MoveAllTo(BPlusTreeInternalPage* node,const KeyType& middle_key,BufferPoolManager* buffer_pool_manager);

  // 将指定位置的节点删除
  void Remove(int index);

 private:
  // Flexible array member for page data.
  MappingType array_[1];  // 页面数据，灵活数组
};
}  // namespace bustub
