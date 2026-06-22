#pragma once

#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <cstdint>
#include <functional>


#include "message_queue_interface.hpp"


namespace message_queue {

template<
  MessageType ValueType,
  ThreadAccessCategory ThreadCategory,
  DeadlockExceptionPolicy ExceptionPolicy,
  typename Compare = std::less<ValueType>
>
class PriorityMessageQueue : public IMessageQueue<ValueType, ThreadCategory, ExceptionPolicy> {
 private:
  struct HeapNode {
    uint64_t sequence_id;
    ValueType data;
  };

  struct MinHeapComparator {
    Compare comp;

    bool operator()(const HeapNode& lhs, const HeapNode& rhs) const {
      if (comp(rhs.data, lhs.data)) {
        return true;
      }
      if (comp(lhs.data, rhs.data)) {
        return false;
      }
      return lhs.sequence_id > rhs.sequence_id;
    }
  };

  std::size_t capacity_;
  std::atomic<std::size_t> size_{0};
  std::atomic<bool> closed_{false};
  uint64_t current_seq_ = 0;

  std::vector<HeapNode> heap_;
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  Compare comp_instance_;
  
};

}