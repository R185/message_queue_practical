#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <semaphore>
#include <type_traits>
#include <utility>
#include <concepts>

#include "message_queue_interface.hpp"

namespace message_queue {

template<ThreadAccessCategory Category>
class CircularQueueSync;

template<>
class CircularQueueSync<ThreadAccessCategory::kSingleProducerSingleConsumer> {
 public:
  struct SyncInfoSend {
    bool acquired = false;
  };

  struct SyncInfoRead {
    bool acquired = false;
  };
 private:
  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;
 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}

  SyncInfoRead AcquireReadOperation() {
    occupied_space_.acquire();
    return SyncInfoRead{true};
  }

  bool TryAcquireReadOperation(SyncInfoRead& info) {
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    info.acquired = true;
    return true;
  }

  void ReleaseSend(SyncInfoSend& info) {
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    } else {
      occupied_space_.release();
    }
  }

  void ReleaseRead(SyncInfoRead& info) {
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
  }

  void CancelSendOperation(SyncInfoSend& info) {
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
  }

  void CancelReadOperation(SyncInfoRead& info) {
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    }
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    return std::forward<Func>(func)();
  }
};

template<>
class CircularQueueSync<ThreadAccessCategory::kMultipleProducerSingleConsumer> {
 public:
  struct SyncInfoSend {
    bool space_acquired = false;
    bool mutex_acquired = false;
  };

  struct SyncInfoRead {
    bool acquired = false;
  };
 private:
  std::mutex send_mutex_;

  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;
 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}
  
  SyncInfoRead AcquireReadOperation() {
    occupied_space_.acquire();
    return SyncInfoRead{true};
  }

  bool TryAcquireReadOperation(SyncInfoRead& info) {
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    info.acquired = true;
    return true;
  }

  void ReleaseSend(SyncInfoSend& info) {
    if (info.mutex_acquired) {
      send_mutex_.unlock();
      info.mutex_acquired = false;
    }
    occupied_space_.release();
    info.space_acquired = false;
  }

  void ReleaseRead(SyncInfoRead& info) {
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
  }

  void CancelSendOperation(SyncInfoSend& info) {
    if (info.mutex_acquired) {
      send_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      free_space_.release();
      info.space_acquired = false;
    }
  }

  void CancelReadOperation(SyncInfoRead& info) {
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    }
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    return std::forward<Func>(func)();
  }
};

template<>
class CircularQueueSync<ThreadAccessCategory::kSingleProducerMultipleConsumer> {
 public:
  struct SyncInfoSend {
    bool acquired = false;
  };

  struct SyncInfoRead {
    bool space_acquired = false;
    bool mutex_acquired = false;
  };

 private:
  std::mutex read_mutex_;
  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;

 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}

  SyncInfoRead AcquireReadOperation() {
    occupied_space_.acquire();
    read_mutex_.lock();
    return SyncInfoRead{true, true};
  }

  bool TryAcquireReadOperation(SyncInfoRead& info) {
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    read_mutex_.lock();
    info = SyncInfoRead{true, true};
    return true;
  }

  void ReleaseSend(SyncInfoSend& info) {
    occupied_space_.release();
    info.acquired = false;
  }

  void ReleaseRead(SyncInfoRead& info) {
    if (info.mutex_acquired) {
      read_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      free_space_.release();
      info.space_acquired = false;
    }
  }

  void CancelSendOperation(SyncInfoSend& info) {
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
  }

  void CancelReadOperation(SyncInfoRead& info) {
    if (info.mutex_acquired) {
      read_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      occupied_space_.release();
      info.space_acquired = false;
    }
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    return std::forward<Func>(func)();
  }
};

template<>
class CircularQueueSync<ThreadAccessCategory::kMultipleProducerMultipleConsumer> {
 public:
  struct SyncInfoSend {
    std::unique_lock<std::mutex> lock;
  };

  struct SyncInfoRead {
    std::unique_lock<std::mutex> lock;
  };

 private:
  mutable std::mutex mutex_;

 public:
  explicit CircularQueueSync(std::size_t) {}

  SyncInfoRead AcquireReadOperation() {
    return SyncInfoRead{std::unique_lock(mutex_)};
  }

  bool TryAcquireReadOperation(SyncInfoRead& info) {
    info.lock = std::unique_lock(mutex_, std::try_to_lock);
    return info.lock.owns_lock();
  }

  void ReleaseSend(SyncInfoSend& info) {
    info.lock.unlock();
  }

  void ReleaseRead(SyncInfoRead& info) {
    info.lock.unlock();
  }

  void CancelSendOperation(SyncInfoSend& info) {
    info.lock.unlock();
  }

  void CancelReadOperation(SyncInfoRead& info) {
    info.lock.unlock();
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    std::lock_guard lock(mutex_);
    return std::forward<Func>(func)();
  }
};

}  // namespace message_queue