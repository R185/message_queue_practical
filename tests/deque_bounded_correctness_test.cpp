#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_deque.hpp"

namespace message_queue::test {
namespace {

template <typename Queue>
class DequeBoundedCorrectnessTest : public testing::Test {
 protected:
  static constexpr std::size_t kCapacity = kSmallCapacity;
};

using BoundedQueueTypes = testing::Types<BoundedSpsc, BoundedMpsc, BoundedSpmc, BoundedMpmc>;
TYPED_TEST_SUITE(DequeBoundedCorrectnessTest, BoundedQueueTypes);

TYPED_TEST(DequeBoundedCorrectnessTest, StartsEmpty) {
  TypeParam queue(this->kCapacity);
  EXPECT_TRUE(queue.Empty());
  EXPECT_EQ(queue.Size(), 0u);
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

TYPED_TEST(DequeBoundedCorrectnessTest, FifoSingleThread) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, static_cast<int>(this->kCapacity) + 1);
  EXPECT_EQ(DrainQueue(queue, this->kCapacity), (std::vector<int>{1, 2, 3, 4}));
  EXPECT_TRUE(queue.Empty());
}

TYPED_TEST(DequeBoundedCorrectnessTest, TrySendAndTryRead) {
  TypeParam queue(this->kCapacity);
  EXPECT_TRUE(queue.TrySend(42));
  ASSERT_EQ(queue.TryRead(), 42);
  EXPECT_EQ(queue.TryRead(), std::nullopt);
}

TYPED_TEST(DequeBoundedCorrectnessTest, FifoAfterPartialDrain) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, 4);
  EXPECT_EQ(queue.Read(), 1);
  queue.Send(4);
  EXPECT_EQ(DrainQueue(queue, 3), (std::vector<int>{2, 3, 4}));
}

TYPED_TEST(DequeBoundedCorrectnessTest, RejectsTrySendWhenFull) {
  TypeParam queue(this->kCapacity);
  FillQueueTry(queue, 0, static_cast<int>(this->kCapacity));
  EXPECT_EQ(queue.Size(), this->kCapacity);
  EXPECT_FALSE(queue.TrySend(999));
}

TYPED_TEST(DequeBoundedCorrectnessTest, CloseDrainsRemaining) {
  TypeParam queue(this->kCapacity);
  FillQueue(queue, 1, 4);
  queue.Close();
  EXPECT_EQ(queue.Size(), 3u);
  EXPECT_EQ(queue.Read(), 1);
  EXPECT_EQ(queue.Read(), 2);
  EXPECT_EQ(queue.Read(), 3);
  EXPECT_TRUE(queue.Empty());
  EXPECT_THROW(queue.Read(), MessageQueueException);
}

TYPED_TEST(DequeBoundedCorrectnessTest, RejectsSendAfterClose) {
  TypeParam queue(this->kCapacity);
  queue.Close();
  EXPECT_THROW(queue.Send(1), MessageQueueException);
  EXPECT_FALSE(queue.TrySend(1));
}

struct Throwing {
  int id = 0;
  bool throw_on_copy = false;
  bool throw_on_move = false;

  Throwing() = default;
  explicit Throwing(int value) : id(value) {}

  Throwing(const Throwing& other)
      : id(other.id), throw_on_copy(other.throw_on_copy), throw_on_move(other.throw_on_move) {
    if (throw_on_copy) {
      throw std::runtime_error("copy");
    }
  }

  Throwing(Throwing&& other)
      : id(other.id), throw_on_copy(other.throw_on_copy), throw_on_move(other.throw_on_move) {
    if (throw_on_move) {
      throw std::runtime_error("move");
    }
  }

  Throwing& operator=(const Throwing&) = default;
  Throwing& operator=(Throwing&&) = default;
};

using ThrowingQueue = DequeBoundedMessageQueue<
    Throwing, ThreadAccessCategory::kMultipleProducerMultipleConsumer,
    DeadlockExceptionPolicy::kNoException>;

TEST(DequeBoundedExceptionSafety, RollsBackReservationWhenStoreThrows) {
  ThrowingQueue queue(1);

  Throwing move_bomb(0);
  move_bomb.throw_on_move = true;
  EXPECT_THROW(queue.Send(std::move(move_bomb)), std::runtime_error);
  EXPECT_EQ(queue.Size(), 0u);
  queue.Send(Throwing(7));
  EXPECT_EQ(queue.Size(), 1u);
  EXPECT_EQ(queue.Read().id, 7);

  Throwing copy_bomb(0);
  copy_bomb.throw_on_copy = true;
  EXPECT_THROW(queue.Send(copy_bomb), std::runtime_error);
  EXPECT_EQ(queue.Size(), 0u);
  EXPECT_TRUE(queue.TrySend(Throwing(9)));
}

TEST(DequeBoundedExceptionSafety, RollsBackReservationWhenPopThrows) {
  ThrowingQueue queue(2);
  Throwing latent(0);
  latent.throw_on_move = true;
  queue.Send(latent);
  EXPECT_EQ(queue.Size(), 1u);
  EXPECT_THROW(queue.Read(), std::runtime_error);
  EXPECT_EQ(queue.Size(), 1u);
}

}  // namespace
}  // namespace message_queue::test
