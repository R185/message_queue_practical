#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_circular.hpp"

namespace message_queue::test {
namespace {

template<typename Queue>
class CircularBufferCorrectnessTest : public testing::Test {
 protected:
  static constexpr std::size_t kCapacity = kSmallCapacity;
};

using BoundedQueueTypes = testing::Types<
    BoundedSpsc, BoundedMpsc, BoundedSpmc, BoundedMpmc>;
using OverwriteQueueTypes = testing::Types<
    OverwriteSpsc, OverwriteMpsc, OverwriteSpmc, OverwriteMpmc>;
using AllQueueTypes = testing::Types<
    BoundedSpsc, BoundedMpsc, BoundedSpmc, BoundedMpmc,
    OverwriteSpsc, OverwriteMpsc, OverwriteSpmc, OverwriteMpmc>;

TYPED_TEST_SUITE(CircularBufferCorrectnessTest, AllQueueTypes);

TYPED_TEST(CircularBufferCorrectnessTest, StartsEmpty) {
  TypeParam queue(this->kCapacity);
  EXPECT_TRUE(queue.Empty());
  EXPECT_EQ(queue.Size(), 0u);
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

TYPED_TEST(CircularBufferCorrectnessTest, FifoSingleThread) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, static_cast<int>(this->kCapacity) + 1);
  auto values = DrainQueue(queue, this->kCapacity);
  EXPECT_EQ(values, (std::vector<int>{1, 2, 3, 4}));
  EXPECT_TRUE(queue.Empty());
}

TYPED_TEST(CircularBufferCorrectnessTest, TrySendAndTryRead) {
  TypeParam queue(this->kCapacity);
  EXPECT_TRUE(queue.TrySend(42));
  ASSERT_EQ(queue.TryRead(), 42);
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

TYPED_TEST(CircularBufferCorrectnessTest, WrapAroundPreservesOrder) {
  TypeParam queue(3);
  FillQueue(queue, 1, 4);
  EXPECT_EQ(queue.Read(), 1);
  queue.Send(4);
  EXPECT_EQ(DrainQueue(queue, 3), (std::vector<int>{2, 3, 4}));
}

TYPED_TEST(CircularBufferCorrectnessTest, CloseResetsState) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, 4);
  queue.Close();
  EXPECT_TRUE(queue.Empty());
  EXPECT_EQ(queue.Size(), 0u);
}

template<typename Queue>
class BoundedCorrectnessTest : public testing::Test {
 protected:
  static constexpr std::size_t kCapacity = kSmallCapacity;
};

TYPED_TEST_SUITE(BoundedCorrectnessTest, BoundedQueueTypes);

TYPED_TEST(BoundedCorrectnessTest, RejectsTrySendWhenFull) {
  TypeParam queue(this->kCapacity);
  FillQueueTry(queue, 0, static_cast<int>(this->kCapacity));
  EXPECT_EQ(queue.Size(), this->kCapacity);
  EXPECT_FALSE(queue.TrySend(999));
}

TYPED_TEST(BoundedCorrectnessTest, BlockingReadUnblocksProducer) {
  TypeParam queue(1);
  queue.Send(1);

  std::atomic<bool> send_finished = false;
  std::thread producer([&] {
    queue.Send(2);
    send_finished = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(send_finished.load());

  EXPECT_EQ(queue.Read(), 1);
  producer.join();
  EXPECT_TRUE(send_finished.load());
  EXPECT_EQ(queue.Read(), 2);
}

template<typename Queue>
class OverwriteCorrectnessTest : public testing::Test {
 protected:
  static constexpr std::size_t kCapacity = 3;
};

TYPED_TEST_SUITE(OverwriteCorrectnessTest, OverwriteQueueTypes);

TYPED_TEST(OverwriteCorrectnessTest, KeepsCapacityAfterOverflow) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, 6);
  EXPECT_EQ(queue.Size(), this->kCapacity);
}

TYPED_TEST(OverwriteCorrectnessTest, EvictsOldestElements) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, 6);
  EXPECT_EQ(DrainQueue(queue, this->kCapacity), (std::vector<int>{3, 4, 5}));
}

TYPED_TEST(OverwriteCorrectnessTest, TrySendAlwaysSucceedsWhenFull) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, static_cast<int>(this->kCapacity) + 1);
  EXPECT_TRUE(queue.TrySend(100));
}

TYPED_TEST(OverwriteCorrectnessTest, SendDoesNotBlockWhenFull) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, static_cast<int>(this->kCapacity) + 1);

  EXPECT_TRUE(RunCompletesWithin(
      [&] { queue.Send(777); },
      std::chrono::milliseconds(200)));
  EXPECT_EQ(queue.Size(), this->kCapacity);
}

}  // namespace
}  // namespace message_queue::test
