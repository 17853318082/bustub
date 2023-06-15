//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {}

/**
 * Helper methods to set/get next page id
 * 获得写一个节点的page_id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 * 获得通过index获取key
 */
INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}
/**
 * 向leaf page中插入数据
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &key_comparator)
    -> int {
  auto distance_in_array = KeyIndex(key, key_comparator);
  // 说明值应该插在最后一个位置
  if (distance_in_array == GetSize()) {
    *(array_ + distance_in_array) = {key, value};
    IncreaseSize(1);   // 长度+1
    return GetSize();  // 返回当前长度
  }
  // 如果插入位置的key值相等，则不做操作
  if (key_comparator(array_[distance_in_array].first, key) == 0) {
    return GetSize();
  }
  // 插入位置在中间，则先后移再插入
  // 执行后移
  std::move_backward(array_ + distance_in_array, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_ + distance_in_array) = {key, value};
  IncreaseSize(1);
  return GetSize();
}
/**
 * 从leaf page中根据获取key所在的位置
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&keyComparator](const auto &pair, auto k) {
    return keyComparator(pair.first, k) < 0;
  });
  return std::distance(array_, target);  // 如果array中没有找到，说明将值插在最后一个位置
}

/**
 * 根据index获取键值对
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(const int index) -> MappingType{
  return array_[index-1];
}
/**
 * 根据给定key查找对应的value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator)
    -> bool {
  // 查找key可能所在的位置
  auto target_index = KeyIndex(key, keyComparator);
  // 因为是叶子节点，所以要么存在相等的key，要么就是不存在
  if (target_index == GetSize() || keyComparator(array_[target_index].first, key) != 0) {
    return false;
  }
  *value = array_[target_index].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int start_index = GetMinSize();  // 从中间开始，到最后结束
  int end_index = GetMaxSize();    // 结束位置为最后
  recipient->ReceiveN(array_ + start_index, end_index - start_index);
  SetSize(start_index);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ReceiveN(MappingType *item, int size) {
  // 复制
  std::copy(item, item + size, array_ + GetSize());
  IncreaseSize(size);
}

/**
 * 从叶节点中删除指定key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
  // 执行删除
  int target_index = KeyIndex(key, keyComparator);
  // 如果没找到
  if (target_index == GetSize()) {
    return GetSize();
  }
  // 向前移动
  std::move(array_ + target_index + 1, array_ + GetSize(), array_ + target_index);
  IncreaseSize(-1);
  return GetSize();
}

/**
 * 将当前节点的第一个拿出，输入节点的最后一个
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToLast(BPlusTreeLeafPage *node) {
  auto first_item = array_[0];
  // 前移一个
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
  // 插入到输入节点的最后一个
  node->InsertLast(first_item);
}

/**
 * 插入到节点的最后一个位置
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const MappingType &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}

/**
 * 将当前节点的最后一个拿出，放到输入节点的第一个
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFirst(BPlusTreeLeafPage *node) {
  auto last_item = array_[GetSize() - 1];
  IncreaseSize(-1);
  node->InsertFirst(last_item);
}

/**
 * 将指定数据插入到节点的第一个
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const MappingType &item) {
  // 后移一位
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *array_ = item;
  IncreaseSize(1);
}
/**
 * 将节点数据全部移动到指定节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *node) {
  node->ReceiveN(array_, GetSize());
  node->SetNextPageId(GetNextPageId());
  SetSize(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
