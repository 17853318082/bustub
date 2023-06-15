//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);                         // 设置当前页面id
  SetParentPageId(parent_id);                 // 设置当前页面的父页面id
  SetMaxSize(max_size);                       // 设置页面的最大尺寸
  SetPageType(IndexPageType::INTERNAL_PAGE);  // 设置页面类型为 internal_page
  SetSize(0);                                 // 设置当前页面的尺寸
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;  // 返回索引为index的key值
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexAt(const ValueType &value) -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  // 返回距离
  return std::distance(array_, it);
}

/**
 * 将index位置的值设置为value
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/**
 * 根据key，查找key所在的子节点
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) const -> ValueType {
  // 查找key子节点的所在位置,由于第0个key为空的
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&keyComparator](const auto &pair, auto k) {
    return keyComparator(pair.first, k) < 0;
  });
  // 查看target_index是否存在于array_中
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  // 如果目标节点的key与给定key相等，则节点返回当前页面
  if (keyComparator(target->first, key) == 0) {
    return target->second;
  }
  // 未能查找到，则返回指定迭代器的前一个
  return std::prev(target)->second;
}
/**
 * 移动一半的数据到另一个节点上
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *pointer,
                                                BufferPoolManager *buffer_pool_manager) {
  // 移动
  int start_index = GetMinSize();
  int end_index = GetMaxSize();
  pointer->ReceiveN(array_, end_index - start_index, buffer_pool_manager);
  SetSize(start_index);
}

/**
 * 接收另一个节点中传过来的N个数据
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReceiveN(MappingType *item, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(item, item + size, array_ + GetSize());
  // 将移动的所有子节点父节点id进行转换
  for (int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i + GetSize()));  // 取出响应的节点
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);  // 取消固定当前页面,页面进行了修改，所以设置为脏页
  }
  IncreaseSize(size);
}

/**
 * 将新旧节点加入到根节点中
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetKeyAt(1, new_key);  // 设置1位置的key为new_key
  SetValueAt(0, old_value);
  SetValueAt(1, new_value);
  SetSize(2);
}

/**
 * 将节点插入到当前节点的指定位置插入新节点
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_node, const KeyType &key,
                                                     const ValueType &new_node) -> int {
  // 执行将元素后移
  auto new_index = IndexAt(old_node);
  std::move_backward(array_ + new_index, array_ + GetSize(), array_ + GetSize() + 1);
  SetKeyAt(new_index, key);
  SetValueAt(new_index, new_node);
  IncreaseSize(1);
  return GetSize();
}

/**
 * 将兄弟节点的第一个移动到当前节点的最后一个位置
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToLast(BPlusTreeInternalPage *node, const KeyType &middle_key,
                                                     BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  auto first_item = array_[0];
  node->InsertLast(first_item, buffer_pool_manager);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}
/**
 * 将指定节点插入到当前节点，并将指定节点的父节点id改为当前节点
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLast(const MappingType &item, BufferPoolManager *buffer_pool_manager) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);

  auto page = buffer_pool_manager->FetchPage(item.second);
  auto* node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(),true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFirst(BPlusTreeInternalPage* node,const KeyType& middle_key,BufferPoolManager* buffer_pool_manager){
  node->SetKeyAt(0,middle_key);
  auto last_item = array_[GetSize()-1];
  node->InsertFirst(last_item,buffer_pool_manager);
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const MappingType &item,BufferPoolManager* buffer_pool_manager){
  // 后移
  std::move_backward(array_,array_+GetSize(),array_+GetSize()+1);
  *array_ = item;
  IncreaseSize(1);
  auto page = buffer_pool_manager->FetchPage(item.second);
  auto *node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(),true);
}

/**
 * 将当前节点的所有元素移动到指定节点
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage* node,const KeyType& middle_key,BufferPoolManager* buffer_pool_manager){
  SetKeyAt(0,middle_key);
  node->ReceiveN(array_,GetSize(),buffer_pool_manager);
  SetSize(0);
}
/**
 * 将指定位置的节点删除
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index){
  // 前移
  std::move(array_+index+1,array_+GetSize(),array_+index);
  IncreaseSize(-1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
