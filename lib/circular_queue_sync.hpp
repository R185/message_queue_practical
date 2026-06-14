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
  std::optional<SyncInfoSend> send_sync_info_;
  std::optional<SyncInfoRead> read_sync_info_;

  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;
 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}

  std::optional<SyncInfoSend>& SendSyncInfo() {
    return send_sync_info_;
  }
  std::optional<SyncInfoRead>& ReadSyncInfo() {
    return read_sync_info_; 
  }

  void AcquireReadOperation() {
    occupied_space_.acquire();
    read_sync_info_ = SyncInfoRead{true};
  }

  bool TryAcquireReadOperation() {
    SyncInfoRead info;
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    info.acquired = true;
    read_sync_info_ = std::move(info);
    return true;
  }

  void ReleaseSend() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    } else {
      occupied_space_.release();
    }
    send_sync_info_.reset();
  }

  void ReleaseRead() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
    read_sync_info_.reset();
  }

  void CancelSendOperation() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
    send_sync_info_.reset();
  }

  void CancelReadOperation() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    }
    read_sync_info_.reset();
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
  std::optional<SyncInfoSend> send_sync_info_;
  std::optional<SyncInfoRead> read_sync_info_;

  std::mutex send_mutex_;
  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;

 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}

  std::optional<SyncInfoSend>& SendSyncInfo() {
    return send_sync_info_;
  }
  std::optional<SyncInfoRead>& ReadSyncInfo() {
    return read_sync_info_;
  }

  void AcquireReadOperation() {
    occupied_space_.acquire();
    read_sync_info_ = SyncInfoRead{true};
  }

  bool TryAcquireReadOperation() {
    SyncInfoRead info;
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    info.acquired = true;
    read_sync_info_ = std::move(info);
    return true;
  }

  void ReleaseSend() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    if (info.mutex_acquired) {
      send_mutex_.unlock();
      info.mutex_acquired = false;
    }
    occupied_space_.release();
    info.space_acquired = false;
    send_sync_info_.reset();
  }

  void ReleaseRead() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
    read_sync_info_.reset();
  }

  void CancelSendOperation() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    if (info.mutex_acquired) {
      send_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      free_space_.release();
      info.space_acquired = false;
    }
    send_sync_info_.reset();
  }

  void CancelReadOperation() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.acquired) {
      occupied_space_.release();
      info.acquired = false;
    }
    read_sync_info_.reset();
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
  std::optional<SyncInfoSend> send_sync_info_;
  std::optional<SyncInfoRead> read_sync_info_;

  std::mutex read_mutex_;
  std::counting_semaphore<> free_space_;
  std::counting_semaphore<> occupied_space_;

 public:
  explicit CircularQueueSync(std::size_t capacity)
    : free_space_(static_cast<std::ptrdiff_t>(capacity))
    , occupied_space_(std::ptrdiff_t{0}) {}

  std::optional<SyncInfoSend>& SendSyncInfo() {
    return send_sync_info_;
  }
  std::optional<SyncInfoRead>& ReadSyncInfo() {
    return read_sync_info_;
  }

  void AcquireReadOperation() {
    occupied_space_.acquire();
    read_mutex_.lock();
    read_sync_info_ = SyncInfoRead{true, true};
  }

  bool TryAcquireReadOperation() {
    SyncInfoRead info;
    if (!occupied_space_.try_acquire()) {
      return false;
    }
    read_mutex_.lock();
    info = SyncInfoRead{true, true};
    read_sync_info_ = std::move(info);
    return true;
  }

  void ReleaseSend() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    occupied_space_.release();
    info.acquired = false;
    send_sync_info_.reset();
  }

  void ReleaseRead() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.mutex_acquired) {
      read_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      free_space_.release();
      info.space_acquired = false;
    }
    read_sync_info_.reset();
  }

  void CancelSendOperation() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    SyncInfoSend& info = *send_sync_info_;
    if (info.acquired) {
      free_space_.release();
      info.acquired = false;
    }
    send_sync_info_.reset();
  }

  void CancelReadOperation() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    SyncInfoRead& info = *read_sync_info_;
    if (info.mutex_acquired) {
      read_mutex_.unlock();
      info.mutex_acquired = false;
    }
    if (info.space_acquired) {
      occupied_space_.release();
      info.space_acquired = false;
    }
    read_sync_info_.reset();
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
  std::optional<SyncInfoSend> send_sync_info_;
  std::optional<SyncInfoRead> read_sync_info_;

  mutable std::mutex mutex_;

 public:
  explicit CircularQueueSync(std::size_t) {}

  std::optional<SyncInfoSend>& SendSyncInfo() {
    return send_sync_info_;
  }
  std::optional<SyncInfoRead>& ReadSyncInfo() {
    return read_sync_info_;
  }

  void AcquireReadOperation() {
    read_sync_info_ = SyncInfoRead{std::unique_lock(mutex_)};
  }

  bool TryAcquireReadOperation() {
    SyncInfoRead info;
    info.lock = std::unique_lock(mutex_, std::try_to_lock);
    if (!info.lock.owns_lock()) {
      return false;
    }
    read_sync_info_ = std::move(info);
    return true;
  }

  void ReleaseSend() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    send_sync_info_->lock.unlock();
    send_sync_info_.reset();
  }

  void ReleaseRead() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    read_sync_info_->lock.unlock();
    read_sync_info_.reset();
  }

  void CancelSendOperation() {
    if (!send_sync_info_.has_value()) {
      return;
    }
    send_sync_info_->lock.unlock();
    send_sync_info_.reset();
  }

  void CancelReadOperation() {
    if (!read_sync_info_.has_value()) {
      return;
    }
    read_sync_info_->lock.unlock();
    read_sync_info_.reset();
  }

  template<std::invocable Func>
  auto ExecuteSynced(Func&& func) const -> std::invoke_result_t<Func&> {
    std::lock_guard lock(mutex_);
    return std::forward<Func>(func)();
  }
};

}  // namespace message_queue