#include <chrono>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_circular.hpp"

namespace message_queue::test {
namespace {

template<typename Queue>
void BecomeBothRoleOnSameThread(Queue& queue, std::size_t capacity) {
  FillQueue(queue, 0, static_cast<int>(capacity));
  EXPECT_EQ(queue.Read(), 0);
  FillQueue(queue, 1000, 1001);
  EXPECT_EQ(queue.Size(), capacity);
}

TEST(CircularBufferDeadlockTest, BoundedOnDeadlockThrowsWhenBothRoleSendOnFull) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnDeadlock> queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_THROW(queue.Send(999), MessageQueueException);
}

TEST(CircularBufferDeadlockTest, BoundedOnRoleChangeThrowsWhenSwitchingRoles) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnThreadRoleChange> queue(kSmallCapacity);
  FillQueue(queue, 0, static_cast<int>(kSmallCapacity));
  EXPECT_THROW(queue.Read(), MessageQueueException);
}

TEST(CircularBufferDeadlockTest, BoundedNoExceptTrySendReturnsFalseWhenBothRoleAndFull) {
  BoundedSpsc queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_FALSE(queue.TrySend(999));
}

TEST(CircularBufferDeadlockTest, BoundedOnDeadlockThrowsWhenBothRoleReadOnEmpty) {
  BoundedWithPolicy<DeadlockExceptionPolicy::kOnDeadlock> queue(kSmallCapacity);
  queue.Send(1);
  queue.Read();
  EXPECT_THROW(queue.Read(), MessageQueueException);
}

TEST(CircularBufferDeadlockTest, BoundedTryReadReturnsNulloptOnEmptyAfterBothRole) {
  BoundedSpsc queue(kSmallCapacity);
  queue.Send(1);
  queue.Read();
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

TEST(CircularBufferDeadlockTest, OverwriteOnDeadlockDoesNotThrowWhenSendingOnFull) {
  OverwriteWithPolicy<DeadlockExceptionPolicy::kOnDeadlock> queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_THROW(queue.Send(999), MessageQueueException);
}

TEST(CircularBufferDeadlockTest, OverwriteTrySendSucceedsWhenFull) {
  OverwriteSpsc queue(kSmallCapacity);
  BecomeBothRoleOnSameThread(queue, kSmallCapacity);
  EXPECT_FALSE(queue.TrySend(999));
}

}  // namespace
}  // namespace message_queue::test
