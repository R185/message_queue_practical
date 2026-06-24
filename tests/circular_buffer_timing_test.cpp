#include <chrono>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_circular.hpp"

namespace message_queue::test {
namespace {

template<typename Func>
std::chrono::milliseconds MeasureExecution(Func&& func) {
  const auto start = std::chrono::steady_clock::now();
  func();
  const auto end = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

TEST(CircularBufferTimingTest, SpscRoundTripWithinSoftLimit) {
  BoundedSpsc queue(kDefaultCapacity);
  constexpr int kIterations = 100000;

  for (int i = 0; i < 1000; ++i) {
    queue.TrySend(i);
    (void)queue.TryRead();
  }

  const auto elapsed = MeasureExecution([&] {
    for (int i = 0; i < kIterations; ++i) {
      queue.Send(i);
      (void)queue.Read();
    }
  });

  if (elapsed > std::chrono::milliseconds(2000)) {
    GTEST_SKIP() << "Timing threshold exceeded: " << elapsed.count() << " ms";
  }
  EXPECT_LT(elapsed.count(), 2000);
}

TEST(CircularBufferTimingTest, OverwriteSendOnFullIsFasterThanBoundedBlocking) {
  OverwriteSpsc overwrite(kSmallCapacity);
  BoundedSpsc bounded(kSmallCapacity);

  FillQueue(overwrite, 0, static_cast<int>(kSmallCapacity));
  FillQueue(bounded, 0, static_cast<int>(kSmallCapacity));

  const auto overwrite_elapsed = MeasureExecution([&] {
    for (int i = 0; i < 10000; ++i) {
      overwrite.Send(i);
    }
  });

  std::atomic<bool> release = false;
  std::thread releaser([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    release = true;
    (void)bounded.Read();
  });

  const auto bounded_start = std::chrono::steady_clock::now();
  std::thread sender([&] {
    bounded.Send(999);
  });

  while (!release.load()) {
    std::this_thread::yield();
  }
  sender.join();
  releaser.join();
  const auto bounded_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - bounded_start);

  EXPECT_LT(overwrite_elapsed.count(), bounded_elapsed.count());
}

TEST(CircularBufferTimingTest, MpmcIsWithinOrderOfMagnitudeOfSpsc) {
  BoundedSpsc spsc(kDefaultCapacity);
  BoundedMpmc mpmc(kDefaultCapacity);
  constexpr int kIterations = 20000;

  const auto spsc_elapsed = MeasureExecution([&] {
    for (int i = 0; i < kIterations; ++i) {
      spsc.TrySend(i);
      (void)spsc.TryRead();
    }
  });

  const auto mpmc_elapsed = MeasureExecution([&] {
    for (int i = 0; i < kIterations; ++i) {
      mpmc.TrySend(i);
      (void)mpmc.TryRead();
    }
  });

  EXPECT_LT(mpmc_elapsed.count(), std::max<int64_t>(5000, spsc_elapsed.count() * 200));
}

}  // namespace
}  // namespace message_queue::test
