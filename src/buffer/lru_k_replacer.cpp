//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation 驱逐满足条件的一个页面
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 * 找到一个最大k次最大的页面并驱逐。仅当页面被标记为evictable时，才能进行驱逐
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * 当页面少于k次访问时，
 * If multiple frames have inf backward k-distance, then evict the frame with the earliest
 * timestamp overall.
 * 如果多个页面为inf的k-distance则驱逐终于在的页面的时间戳
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 * 成功驱逐一个页面应该应该减小replacer的curr_size_并且移除页面的访问历史
 * @param[out] frame_id id of frame that is evicted.
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 加锁
  std::scoped_lock<std::mutex> lock(latch_);
  // 如果没有页面可被驱逐，则返回false
  if (curr_size_ == 0) {
    return false;
  }
  // 先从历史队列中驱逐
  for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
    // 查看该页面是否可驱逐，如果可驱逐则进行驱逐
    if (is_evictable[*it]) {
      // 执行驱逐
      history_list_.erase(it);
      curr_size_--;
      count_[*it] = 0;            // 访问记录清空
      is_evictable[*it] = false;  // 标记为不可驱逐
      *frame_id = *it;
      return true;
    }
  }
  // 如果历史队列没有可驱逐页面，则从缓存队列中驱逐
  for (auto it = cache_list_.begin(); it != cache_list_.end(); it++) {
    // 可驱逐，则驱逐当前页面
    if (is_evictable[*it]) {
      cache_list_.erase(it);
      curr_size_--;
      count_[*it] = 0;
      is_evictable[*it] = false;
      *frame_id = *it;
      return true;
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 * 记录给定页面id当前进入的时间戳。如果再次之前没有进入记录则将当前页面id加入到记录中(调整缓存队列中的页面位置，将传入页面调整到队首)
 *
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 * 如果页面id是无效的，则抛出异常。
 *
 * @param frame_id id of frame that received a new access.
 */

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // 加锁
  std::scoped_lock<std::mutex> lock(latch_);
  // 如果当前访问页面id超过了页面数量，则抛出异常
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  count_[frame_id]++;
  // 首先查看当前页面在访问列表中的次数，如果大于K的加入到缓存列表
  if (count_[frame_id] == k_) {
    // 此时应该将当前页面加入缓存队列，从访问历史队列中将其去除
    auto it = std::find(history_list_.begin(), history_list_.end(), frame_id);
    if (it != history_list_.end()) {
      history_list_.erase(it);
    }
    // 将其加入到缓存队列
    cache_list_.push_front(frame_id);
  } else if (count_[frame_id] > k_) {
    // 当前说明，该页面已经在缓存队列中了，如果没有在缓存队列中，需要重新加入缓存队列
    auto it = std::find(cache_list_.begin(), cache_list_.end(), frame_id);
    // 说明已经在缓存队列当中，将其挪到队首
    if (it != cache_list_.end()) {
      cache_list_.erase(it);
      cache_list_.push_front(frame_id);
    } else {
      cache_list_.push_front(frame_id);
    }
  } else {
    // 当前说明，该页面访问次数不足k_，即页面不能加入缓存队列，如果当前页面不再访问历史中，则加入访问历史
    if (count_.count(frame_id) == 0) {
      history_list_.push_front(frame_id);
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 * 标记一个页面是否可进行驱逐，该函数可以控制置换器的大小(可驱逐的页面数量)，
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 * 如果一个页面是之前是驱逐的，并且被设置为不可驱逐，则替换器的带线啊哦应该减少，如果一个页面之前是
 * 不可驱逐的，并且被设置为可驱逐，则置换器的大小应该增加
 * If frame id is invalid, throw an exception or abort the process.
 * 如果页面id的无效的，则抛出一个异常 frame_id >raplacer_size;
 * For other scenarios, this function should terminate without modifying anything.
 * 对于其他场景，该函数不做操作
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 加锁
  std::scoped_lock<std::mutex> lock(latch_);
  // frame_id 是否有效
  if (frame_id > this->Size()) {
    throw std::exception();
  }
  // 查看驱逐设置 --> 将页面设置为可驱逐
  if (set_evictable) {
    // 之前为不可驱逐，设置为可驱逐，并且curr_size_++;
    if (!is_evictable[frame_id]) {
      is_evictable[frame_id] = set_evictable;
      curr_size_++;
    }
  } else {
    // 之前未可驱逐，设置为不可驱逐，并且curr_size_--;
    if(is_evictable[frame_id]){
      is_evictable[frame_id] = set_evictable;
      curr_size_--;
    }
  }
  // 其他情况不做处理
}

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */

void LRUKReplacer::Remove(frame_id_t frame_id) {

}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
