//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 * 在缓冲池中创建一个新的页面。设置page_id为新页面的id，如果所有frame都当前被固定并因此不能用于清除，则返回一个空指针。
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 * 你应该从空闲列表或替换器中选择要替换的frame页面（总是首先从空闲列表中查找），然后调用AllocatePage()方法获取一个新的页面ID。
 * 如果要替换的frame有一个脏页（即已被修改但尚未写回磁盘），你应该首先将其写回磁盘。你还需要重置新页面的内存和元数据。
 * Remember to "Pin" the frame by calling replace r.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 * 记得通过调用replacer.SetEvictable(frame_id,
 * false)方法来“固定”（Pin）frame，以便在缓冲池管理器“取消固定”（Unpin）之前，
 * 替换器不会将其清除。此外，还要在替换器中记录frame的访问历史，以便lru-k算法能够正常工作。
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 加锁
  std::scoped_lock<std::mutex> lock(latch_);
  bool is_remove = false;
  // 查看是否有页面被清除
  for (size_t i = 0; i < pool_size_; i++) {
    // 说明当前页面可以被清除，则进行清除
    if (pages_[i].GetPinCount() == 0) {
      is_remove = true;
      break;
    }
  }
  // 如果有页面可被清除，则执行清除
  if (is_remove) {
    frame_id_t frame_id;
    // 优先查看free_list_中是否有空闲页面
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      // 从置换器中查找替换页
      replacer_->Evict(&frame_id);
      page_id_t evict_page_id = pages_[frame_id].GetPageId();
      // 查看要替换的页面是否是一个脏页,如果是将修改写入磁盘
      if (pages_[frame_id].IsDirty()) {
        this->disk_manager_->WritePage(evict_page_id, pages_[frame_id].GetData());  // 写入磁盘
        pages_[frame_id].is_dirty_ = false;
      }
      // 刷新页面
      pages_[frame_id].ResetMemory();
      // 从hash表中移除
      page_table_->Remove(evict_page_id);
    }
    // 创建页面
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // 否则返回空指针
  return nullptr;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 * 从缓冲池中获取所请求的页面。如果page_id需要从磁盘获取，但所有frame当前都在使用且不可清除（换句话说，被固定），则返回nullptr。
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 * 首先在缓冲池中搜索page_id。如果没有找到，则从空闲列表或替换器中选择一个替换的frame（总是首先从空闲列表中查找），
 * 通过调用disk_manager_->ReadPage()方法从磁盘中读取页面，并将旧页面替换到该frame中。
 * 与NewPgImp()类似，如果旧页面是脏页，你需要将它写回磁盘并更新新页面的元数据。
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 * 此外，记得像NewPgImp()函数一样禁用驱逐并记录frame的访问历史
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 从缓冲池中获取请求页面，如果页面在磁盘中，则需要新建一个页面
  // 查看页面是否在缓存池中
  frame_id_t frame_id;
  // 说明缓存池中有该页，返回该页面
  if (page_table_->Find(page_id, frame_id)) {
    // 更新页面访问记录
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // 缓存池中没有该页,说明在磁盘中->新建一个页面，并返回该页面
  bool is_remove = false;
  // 查看是否有页面被清除
  for (size_t i = 0; i < pool_size_; i++) {
    // 说明当前页面可以被清除，则进行清除
    if (pages_[i].GetPinCount() == 0) {
      is_remove = true;
      break;
    }
  }
  // 如果有页面可被清除，则执行清除
  if (is_remove) {
    frame_id_t frame_id;
    // 优先查看free_list_中是否有空闲页面
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      // 从置换器中查找替换页
      replacer_->Evict(&frame_id);
      page_id_t evicted_page_id = pages_[frame_id].GetPageId();
      // 查看要替换的页面是否是一个脏页,如果是将修改写入磁盘
      if (pages_[frame_id].IsDirty()) {
        this->disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());  // 写入磁盘
        pages_[frame_id].is_dirty_ = false;
      }
      // 刷新页面
      pages_[frame_id].ResetMemory();
      // 从hash表中移除
      page_table_->Remove(evicted_page_id);
    }
    // 创建页面--不需要分配页面id
    page_table_->Insert(page_id, frame_id);

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    // 从磁盘读入缓存
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // 否则返回空指针
  return nullptr;
}

/**
 * TODO(P1): Add implementation
 * 从缓冲池中取消固定（Unpin）目标页面,pin_count--;
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 * 从缓冲池中取消固定（Unpin）目标页面。如果page_id不在缓冲池中或它的固定计数已经为0，则返回false。
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 * 减少页面的固定计数。如果固定计数达到了0，则替换器中设置为可驱逐该frame。此外，设置页面的脏标志以指示页面是否已被修改。
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * 如果页面应该被标记为脏页，则返回true，否则返回false
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 * 如果页面不在页面表中或者在调用此函数之前其固定计数<=0，则返回false，否则返回true。
 */

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 查看缓冲池中是否有当前页面
  if (page_table_->Find(page_id, frame_id)) {
    if (is_dirty) {
      pages_[frame_id].is_dirty_ = is_dirty;  // 设置页面脏标志
    }
    // 有当前页面在页面列表中，查看当前页面的pincount是否为0，如果为0返回false
    if (pages_[frame_id].GetPinCount() > 0) {
      // 减少固定计数
      pages_[frame_id].pin_count_--;
      // 如果固定计数达到了0，则替换其应该清除frame
      if (pages_[frame_id].GetPinCount() <= 0) {
        // 设置为可驱逐
        replacer_->SetEvictable(frame_id, true);
      }
      return true;
    }
    // 固定计数已经为0
    return false;
  }
  // 不再缓冲列表中
  return false;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 * 从磁盘中刷新目标页
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 * 使用DiskManager::WritePage()方法将页面刷新到磁盘，而不考虑页面的脏标志。在刷新后取消页面的脏标志。
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 * 如果在页面表中找不到页面，则返回false，否则返回true。
 */

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 查看页面是否在缓存中
  frame_id_t frame_id;
  // 如果不再缓存中则返回false
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // 在缓存中，则刷新页面到磁盘中
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  // 取消页面脏标志
  pages_[frame_id].is_dirty_ = false;
  return true;
}

// 刷新所有页面到磁盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  // 刷新所有页面
  for (size_t i = 0; i < pool_size_; i++) {
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].is_dirty_ = false;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 * 删除一个页面从缓冲池中，如果页面不在缓冲池中，则什么也不做并返回true，如果页面是被固定的则不能被删除并立刻返回false
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 * 从页面列表删除页面之后，停止从置换器中跟踪该frame，并将frame重新添加到空闲列表中
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 * 如果页面存在但不能被删除则返回false，如果页面不存在或者删除成功则返回true
 */

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 查看是否在缓存池中
  if (page_table_->Find(page_id, frame_id)) {
    // 查看页面是否被固定,说明被固定
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }
    // 不被固定则删除,从置换器中移除
    replacer_->SetEvictable(frame_id, true);

    pages_[frame_id].ResetMemory();
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;

    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    page_table_->Remove(page_id);  // 从页面列表中删除
    return true;
  }
  // 不在则返回true
  return true;
}

/**
 * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
 * 在磁盘上分配一个页面。调用者在调用此函数之前应该获取latch锁。返回分配的页面ID。
 * @return the id of the allocated page
 */
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
