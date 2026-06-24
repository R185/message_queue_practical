#include <atomic>
#include <barrier>
#include <chrono>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>

#include <gtest/gtest.h>

#include "test_helpers_common.hpp"
#include "test_helpers_circular.hpp"

namespace message_queue::test {
namespace {

template<typename Queue>
void RunSpscFifo(Queue& queue, int message_count) {
  std::barrier start_barrier(2);

  std::thread producer([&] {
    start_barrier.arrive_and_wait();
    for (int i = 0; i < message_count; ++i) {
      queue.Send(i);
    }
  });

  std::vector<int> received;
  received.reserve(message_count);
  start_barrier.arrive_and_wait();
  for (int i = 0; i < message_count; ++i) {
    received.push_back(queue.Read());
  }

  producer.join();
  EXPECT_EQ(received.size(), static_cast<std::size_t>(message_count));
  for (int i = 0; i < message_count; ++i) {
    EXPECT_EQ(received[static_cast<std::size_t>(i)], i);
  }
}

template<typename Queue>
void RunSpscOverwritePressure(Queue& queue, int message_count) {
  std::atomic<bool> producer_done{false};

  std::thread producer([&] {
    for (int i = 0; i < message_count; ++i) {
      queue.Send(i);
    }
    producer_done.store(true, std::memory_order_release);
  });

  while (!producer_done.load(std::memory_order_acquire)) {
    while (queue.TryRead()) {
    }
    std::this_thread::yield();
  }
  producer.join();

  EXPECT_LE(queue.Size(), kDefaultCapacity);
}

template<typename Queue>
void RunOverwritePressure(Queue& queue, int message_count) {
  std::atomic<bool> producer_done{false};

  std::thread producer([&] {
    for (int i = 0; i < message_count; ++i) {
      queue.Send(i);
    }
    producer_done.store(true, std::memory_order_release);
  });

  while (!producer_done.load(std::memory_order_acquire)) {
    while (queue.TryRead()) {
    }
    std::this_thread::yield();
  }
  producer.join();

  EXPECT_LE(queue.Size(), kDefaultCapacity);
}

template<typename Queue>
void RunMpscUniqueValues(Queue& queue, int producers, int per_producer) {
  const int total = producers * per_producer;
  std::barrier start_barrier(producers + 1);
  std::atomic<int> consumed = 0;
  std::mutex values_mutex;
  std::multiset<int> received;

  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(producers));
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
    {
      std::lock_guard lock(values_mutex);
      received.insert(value);
    }
  }
  consumed.store(total);

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

template<typename Queue>
void RunSpmcUniqueValues(Queue& queue, int consumers, int total_messages) {
  std::barrier start_barrier(consumers + 1);
  std::atomic<int> produced = 0;
  std::atomic<int> consumed = 0;
  std::mutex values_mutex;
  std::multiset<int> received;
  std::thread producer([&] {
    start_barrier.arrive_and_wait();
    for (int i = 0; i < total_messages; ++i) {
      queue.Send(i);
      produced.fetch_add(1);
    }
  });

  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(consumers));
  for (int i = 0; i < consumers; ++i) {
    threads.emplace_back([&] {
      start_barrier.arrive_and_wait();
      while (true) {
        auto value = queue.TryRead();
        if (!value.has_value()) {
          if (consumed.load() >= total_messages) {
            break;
          }
          std::this_thread::yield();
          continue;
        }
        {
          std::lock_guard lock(values_mutex);
          received.insert(*value);
        }
        if (consumed.fetch_add(1) + 1 >= total_messages) {
          break;
        }
      }
    });
  }

  producer.join();
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(produced.load(), total_messages);
  EXPECT_EQ(received.size(), static_cast<std::size_t>(total_messages));
  for (int i = 0; i < total_messages; ++i) {
    EXPECT_EQ(received.count(i), 1u);
  }
}

template<typename Queue>
void RunMpmcUniqueValues(Queue& queue, int producers, int consumers, int per_producer) {
  const int total = producers * per_producer;
  std::barrier start_barrier(producers + consumers + 1);
  std::atomic<int> consumed = 0;
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

  for (int i = 0; i < consumers; ++i) {
    threads.emplace_back([&] {
      start_barrier.arrive_and_wait();
      while (true) {
        const int ticket = consumed.fetch_add(1);
        if (ticket >= total) {
          consumed.fetch_sub(1);
          break;
        }
        const int value = queue.Read();
        {
          std::lock_guard lock(values_mutex);
          received.insert(value);
        }
      }
    });
  }

  start_barrier.arrive_and_wait();
  for (int producer_id = 0; producer_id < producers; ++producer_id) {
    threads[static_cast<std::size_t>(producer_id)].join();
  }
  for (int i = 0; i < consumers; ++i) {
    threads[static_cast<std::size_t>(producers + i)].join();
  }

  EXPECT_EQ(received.size(), static_cast<std::size_t>(total));
}

template<typename QueuePair>
class CircularBufferSpscConcurrencyTest : public testing::Test {};

template<typename QueuePair>
class CircularBufferMpscConcurrencyTest : public testing::Test {};

template<typename QueuePair>
class CircularBufferSpmcConcurrencyTest : public testing::Test {};

template<typename QueuePair>
class CircularBufferMpmcConcurrencyTest : public testing::Test {};

using SpscConcurrencyPairs = testing::Types<std::pair<BoundedSpsc, OverwriteSpsc>>;
using MpscConcurrencyPairs = testing::Types<std::pair<BoundedMpsc, OverwriteMpsc>>;
using SpmcConcurrencyPairs = testing::Types<std::pair<BoundedSpmc, OverwriteSpmc>>;
using MpmcConcurrencyPairs = testing::Types<std::pair<BoundedMpmc, OverwriteMpmc>>;

TYPED_TEST_SUITE(CircularBufferSpscConcurrencyTest, SpscConcurrencyPairs);
TYPED_TEST_SUITE(CircularBufferMpscConcurrencyTest, MpscConcurrencyPairs);
TYPED_TEST_SUITE(CircularBufferSpmcConcurrencyTest, SpmcConcurrencyPairs);
TYPED_TEST_SUITE(CircularBufferMpmcConcurrencyTest, MpmcConcurrencyPairs);

TYPED_TEST(CircularBufferSpscConcurrencyTest, FifoBoundedAndOverwritePressure) {
  typename TypeParam::first_type bounded(kDefaultCapacity);
  typename TypeParam::second_type overwrite(kDefaultCapacity);
  RunSpscFifo(bounded, kStressMessageCount);
  RunSpscOverwritePressure(overwrite, kStressMessageCount);
}

TYPED_TEST(CircularBufferMpscConcurrencyTest, WithoutLossAndOverwritePressure) {
  typename TypeParam::first_type bounded(kDefaultCapacity);
  typename TypeParam::second_type overwrite(kDefaultCapacity);
  RunMpscUniqueValues(bounded, 4, 100);
  RunOverwritePressure(overwrite, 1000);
}

TYPED_TEST(CircularBufferSpmcConcurrencyTest, WithoutLossAndOverwritePressure) {
  typename TypeParam::first_type bounded(kDefaultCapacity);
  typename TypeParam::second_type overwrite(kDefaultCapacity);
  RunSpmcUniqueValues(bounded, 4, 1000);
  RunOverwritePressure(overwrite, 1000);
}

TYPED_TEST(CircularBufferMpmcConcurrencyTest, WithoutLossAndOverwritePressure) {
  typename TypeParam::first_type bounded(kDefaultCapacity);
  typename TypeParam::second_type overwrite(kDefaultCapacity);
  RunMpmcUniqueValues(bounded, 4, 4, 250);
  RunOverwritePressure(overwrite, 1000);
}

TEST(CircularBufferConcurrencyStandaloneTest, BoundedMpscFourProducersMainConsumer) {
  BoundedMpsc queue(kDefaultCapacity);
  const int producers = 4;
  const int per_producer = 100;
  const int total = producers * per_producer;
  std::barrier start_barrier(producers + 1);
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
    received.insert(queue.Read());
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(received.size(), static_cast<std::size_t>(total));
}

TEST(CircularBufferConcurrencyStandaloneTest, BoundedProducerBlocksWithoutDataLoss) {
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

TEST(CircularBufferConcurrencyStandaloneTest, OverwriteKeepsLatestValuesUnderPressure) {
  OverwriteSpsc queue(4);
  std::atomic<bool> done = false;

  std::thread producer([&] {
    for (int i = 0; i < 1000; ++i) {
      queue.Send(i);
    }
    done = true;
  });

  while (!done.load()) {
    while (auto value = queue.TryRead()) {
      (void)value;
    }
    std::this_thread::yield();
  }
  producer.join();

  EXPECT_LE(queue.Size(), 4u);
}

}  // namespace
}  // namespace message_queue::test
