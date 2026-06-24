#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace message_queue::test {

inline constexpr std::size_t kSmallCapacity = 4;
inline constexpr std::size_t kDefaultCapacity = 16;
inline constexpr int kStressMessageCount = 10000;

template<typename Queue>
void FillQueue(Queue& queue, int from, int to) {
  for (int value = from; value < to; ++value) {
    queue.Send(value);
  }
}

template<typename Queue>
void FillQueueTry(Queue& queue, int from, int to) {
  for (int value = from; value < to; ++value) {
    ASSERT_TRUE(queue.TrySend(value));
  }
}

template<typename Queue>
std::vector<int> DrainQueue(Queue& queue, std::size_t count) {
  std::vector<int> values;
  values.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    values.push_back(queue.Read());
  }
  return values;
}

template<typename Queue>
std::vector<int> DrainQueueTry(Queue& queue) {
  std::vector<int> values;
  while (auto value = queue.TryRead()) {
    values.push_back(*value);
  }
  return values;
}

template<typename Func>
bool RunCompletesWithin(Func&& func, std::chrono::milliseconds timeout) {
  auto future = std::async(std::launch::async, std::forward<Func>(func));
  return future.wait_for(timeout) == std::future_status::ready;
}

template<typename Func>
bool RunDoesNotCompleteWithin(Func&& func, std::chrono::milliseconds timeout) {
  auto future = std::async(std::launch::async, std::forward<Func>(func));
  return future.wait_for(timeout) == std::future_status::timeout;
}

}  // namespace message_queue::test
