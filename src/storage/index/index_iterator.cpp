/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index = 0)
    : buffer_pool_manager_(bmp), page_(page), index_(index) {
  if (page_ != nullptr) {
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
  } else {
    leaf_ = nullptr;
  }
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // 判断是否是最后一个节点
  return leaf_->GetNextPageId == INVALID_PAGE_ID && index_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (leaf_ != nullptr) {
    return leaf_->GetItem(index_);
  }
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    // 需要换页
    if(leaf_->GetNextPageId != INVALID_PAGE_ID && index_ == leaf->GetSize()-1){
        auto next_page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
        next_page->RLatch();
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_->GetPageId(),false);
        page_ = next_page;
        leaf_ = reinterpret_cast<LeafPage*>(page_->GetData()); // 转换叶子节点
        index_ = 0;
    }else{
        return index++;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
