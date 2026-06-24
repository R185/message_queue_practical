#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_deque.hpp"

namespace message_queue::test {
namespace {

template <typename Queue>
void RunSpscFifo(Queue& queue, int message_count) {
  std::barrier start_barrier(2);
  std::thread producer([&] {
    start_barrier.arrive_and_wait();
    for (int i = 0; i < message_count; ++i) {
      queue.Send(i);
    }
  });

  std::vector<int> received;
  received.reserve(static_cast<std::size_t>(message_count));
  start_barrier.arrive_and_wait();
  for (int i = 0; i < message_count; ++i) {
    received.push_back(queue.Read());
  }
  producer.join();

  ASSERT_EQ(received.size(), static_cast<std::size_t>(message_count));
  for (int i = 0; i < message_count; ++i) {
    EXPECT_EQ(received[static_cast<std::size_t>(i)], i);
  }
}

template <typename Queue>
void RunMpscUniqueValues(Queue& queue, int producers, int per_producer) {
  const int total = producers * per_producer;
  std::barrier start_barrier(producers + 1);
  std::mutex values_mutex;
  std::multiset<int> received;

  std::vector<std::thread> threads;
  for (int producer_id = 0; producer_id < producers; ++producer_id) {
    threads.emplace_back([&, producer_id] {
      start_barrier.arrive_and_wait();
      const int base = producer_id * 1000;
      for (int i = 0; i < per_producer; ++i) {
        queue.Send(base + i);
      }
    });
  }

  start_barrier.arrive_and_wait();
  for (int i = 0; i < total; ++i) {
    const int value = queue.Read();
    std::lock_guard lock(values_mutex);
    received.insert(value);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(received.size(), static_cast<std::size_t>(total));
  for (int producer_id = 0; producer_id < producers; ++producer_id) {
    for (int i = 0; i < per_producer; ++i) {
      EXPECT_EQ(received.count(producer_id * 1000 + i), 1u);
    }
  }
}

template <typename Queue>
void RunSpmcUniqueValues(Queue& queue, int consumers, int total_messages) {
  std::barrier start_barrier(consumers + 1);
  std::atomic<int> ticket = 0;
  std::mutex values_mutex;
  std::multiset<int> received;

  std::thread producer([&] {
    start_barrier.arrive_and_wait();
    for (int i = 0; i < total_messages; ++i) {
      queue.Send(i);
    }
  });

  std::vector<std::thread> threads;
  for (int c = 0; c < consumers; ++c) {
    threads.emplace_back([&] {
      start_barrier.arrive_and_wait();
      while (ticket.fetch_add(1) < total_messages) {
        const int value = queue.Read();
        std::lock_guard lock(values_mutex);
        received.insert(value);
      }
    });
  }

  producer.join();
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(received.size(), static_cast<std::size_t>(total_messages));
  for (int i = 0; i < total_messages; ++i) {
    EXPECT_EQ(received.count(i), 1u);
  }
}

template <typename Queue>
void RunMpmcUniqueValues(Queue& queue, int producers, int consumers, int per_producer) {
  const int total = producers * per_producer;
  std::barrier start_barrier(producers + consumers + 1);
  std::atomic<int> ticket = 0;
  std::mutex values_mutex;
  std::multiset<int> received;

  std::vector<std::thread> threads;
  for (int producer_id = 0; producer_id < producers; ++producer_id) {
    threads.emplace_back([&, producer_id] {
      start_barrier.arrive_and_wait();
      const int base = producer_id * 1000;
      for (int i = 0; i < per_producer; ++i) {
        queue.Send(base + i);
      }
    });
  }
  for (int c = 0; c < consumers; ++c) {
    threads.emplace_back([&] {
      start_barrier.arrive_and_wait();
      while (ticket.fetch_add(1) < total) {
        const int value = queue.Read();
        std::lock_guard lock(values_mutex);
        received.insert(value);
      }
    });
  }

  start_barrier.arrive_and_wait();
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(received.size(), static_cast<std::size_t>(total));
}

TEST(DequeBoundedConcurrency, SpscFifo) {
  BoundedSpsc queue(kDefaultCapacity);
  RunSpscFifo(queue, kStressMessageCount);
}

TEST(DequeBoundedConcurrency, MpscUniqueValues) {
  BoundedMpsc queue(kDefaultCapacity);
  RunMpscUniqueValues(queue, 4, 1000);
}

TEST(DequeBoundedConcurrency, SpmcUniqueValues) {
  BoundedSpmc queue(kDefaultCapacity);
  RunSpmcUniqueValues(queue, 4, 4000);
}

TEST(DequeBoundedConcurrency, MpmcUniqueValues) {
  BoundedMpmc queue(kDefaultCapacity);
  RunMpmcUniqueValues(queue, 4, 4, 1000);
}

TEST(DequeBoundedConcurrency, ProducerBlocksWithoutDataLoss) {
  BoundedSpsc queue(2);
  std::atomic<bool> blocked_seen = false;

  std::thread producer([&] {
    queue.Send(1);
    queue.Send(2);
    blocked_seen = true;
    queue.Send(3);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_TRUE(blocked_seen.load());

  EXPECT_EQ(queue.Read(), 1);
  EXPECT_EQ(queue.Read(), 2);
  producer.join();
  EXPECT_EQ(queue.Read(), 3);
}

TEST(DequeBoundedConcurrency, BlockedSendIsUnblockedByClose) {
  BoundedSpsc queue(1);
  queue.Send(1);

  std::atomic<bool> threw{false};
  std::thread producer([&] {
    try {
      queue.Send(2);
    } catch (const MessageQueueException&) {
      threw.store(true);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  queue.Close();
  producer.join();

  EXPECT_TRUE(threw.load());
}

}  // namespace
}  // namespace message_queue::test
