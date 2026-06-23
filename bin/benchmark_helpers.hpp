#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#if defined(_M_X64) || defined(_M_IX86) || defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "circular/circular_bounded_message_queue.hpp"
#include "circular/circular_overwrite_oldest_message_queue.hpp"
#include "deque_bounded_message_queue.hpp"
#include "message_queue_interface.hpp"

namespace message_queue::bench {

struct BenchmarkMessage {
  std::uint64_t seq{0};
  std::uint64_t producer_id{0};
  std::uint64_t send_ns{0};
};

enum class LoadProfile {
  kBalanced,
  kFastProducer,
  kFastConsumer,
  kBatch,
  kBurst,
  kPriorityMix
};

enum class Topology {
  kSpsc,
  kMpsc,
  kSpmc,
  kMpmc
};

struct ThreadLayout {
  int producers{1};
  int consumers{1};
  int hw_threads{1};
  double thread_ratio{0.5};
};

inline int EnvRead(const char* name, int default_value) {
  const char* value = std::getenv(name);
  if (!value) {
    return default_value;
  }
  return std::atoi(value);
}

inline double EnvReadDouble(const char* name, double default_value) {
  const char* value = std::getenv(name);
  if (!value) {
    return default_value;
  }
  return std::atof(value);
}

inline bool EnvFlag(const char* name, bool default_value = false) {
  const char* value = std::getenv(name);
  if (!value) {
    return default_value;
  }
  const std::string text(value);
  return text == "1" || text == "true" || text == "TRUE" || text == "on" || text == "ON";
}

inline ThreadLayout ThreadsForTopology(Topology topology) {
  const int hw_threads = std::max(1, static_cast<int>(std::thread::hardware_concurrency()));
  const bool use_all_threads = EnvFlag("MQ_BENCH_USE_ALL_THREADS", false);
  const double ratio_default = use_all_threads ? 1.0 : 0.5;
  const double ratio = std::clamp(EnvReadDouble("MQ_BENCH_THREAD_RATIO", ratio_default), 0.1, 1.0);
  const int workers = std::max(2, static_cast<int>(std::floor(static_cast<double>(hw_threads) * ratio)));

  ThreadLayout layout{};
  layout.hw_threads = hw_threads;
  layout.thread_ratio = ratio;

  switch (topology) {
    case Topology::kSpsc:
      layout.producers = 1;
      layout.consumers = 1;
      break;
    case Topology::kMpsc:
      layout.producers = std::max(1, workers - 1);
      layout.consumers = 1;
      break;
    case Topology::kSpmc:
      layout.producers = 1;
      layout.consumers = std::max(1, workers - 1);
      break;
    case Topology::kMpmc:
      layout.producers = std::max(1, workers / 2);
      layout.consumers = std::max(1, workers - layout.producers);
      break;
  }

  const int producers_override = EnvRead("MQ_BENCH_PRODUCERS", -1);
  const int consumers_override = EnvRead("MQ_BENCH_CONSUMERS", -1);
  if (producers_override > 0) {
    layout.producers = producers_override;
  }
  if (consumers_override > 0) {
    layout.consumers = consumers_override;
  }
  return layout;
}

inline ThreadAccessCategory CategoryForTopology(Topology topology) {
  switch (topology) {
    case Topology::kSpsc:
      return ThreadAccessCategory::kSingleProducerSingleConsumer;
    case Topology::kMpsc:
      return ThreadAccessCategory::kMultipleProducerSingleConsumer;
    case Topology::kSpmc:
      return ThreadAccessCategory::kSingleProducerMultipleConsumer;
    case Topology::kMpmc:
      return ThreadAccessCategory::kMultipleProducerMultipleConsumer;
  }
  return ThreadAccessCategory::kSingleProducerSingleConsumer;
}

struct ScenarioParams {
  std::size_t capacity{1024};
  int producers{1};
  int consumers{1};
  int hw_threads{1};
  int total_messages{100000};
  LoadProfile profile{LoadProfile::kBalanced};
  int scenario_timeout_sec{120};
  double thread_ratio{0.5};
  bool no_backoff{false};
};

struct ScenarioMetrics {
  std::uint64_t produced{0};
  std::uint64_t delivered{0};
  std::uint64_t lost{0};
  std::uint64_t seq_gaps{0};
  std::uint64_t blocked_send_attempts{0};
  std::uint64_t blocked_read_attempts{0};
  std::uint64_t cas_retries{0};
  double elapsed_sec{0.0};
  double produce_per_s{0.0};
  double consume_per_s{0.0};
  double send_p50_ns{0.0};
  double send_p99_ns{0.0};
  double read_p50_ns{0.0};
  double read_p99_ns{0.0};
  double producer_fairness{1.0};
  double consumer_fairness{1.0};
  double fairness_ratio{1.0};
  bool timed_out{false};
  std::size_t queue_size_at_end{0};
  double producer_threads{1.0};
  double consumer_threads{1.0};
  double hw_threads{1.0};
  double thread_ratio{0.5};
};

template<typename QueueType>
concept HasQueueStats = requires(const QueueType& queue) {
  { queue.Stats().cas_retries } -> std::convertible_to<std::size_t>;
};

template<typename QueueType>
concept HasTryEnqueue = requires(QueueType& queue, BenchmarkMessage message) {
  { queue.TryEnqueue(std::move(message)) } -> std::convertible_to<bool>;
};

template<typename QueueType>
concept HasTryDequeue = requires(QueueType& queue) {
  { queue.TryDequeue() } -> std::convertible_to<std::optional<BenchmarkMessage>>;
};

template<typename QueueType>
class IMessageQueueAdapter {
 public:
  explicit IMessageQueueAdapter(std::size_t capacity)
      : queue_(capacity) {}

  bool TrySend(BenchmarkMessage message) {
    if constexpr (HasTryEnqueue<QueueType>) {
      return queue_.TryEnqueue(std::move(message));
    }
    return queue_.TrySend(std::move(message));
  }

  std::optional<BenchmarkMessage> TryRead() {
    if constexpr (HasTryDequeue<QueueType>) {
      return queue_.TryDequeue();
    }
    return queue_.TryRead();
  }

  std::size_t Size() const {
    return queue_.Size();
  }

  std::uint64_t CasRetries() const {
    if constexpr (HasQueueStats<QueueType>) {
      return queue_.Stats().cas_retries;
    }
    return 0;
  }

 private:
  QueueType queue_;
};

inline std::uint64_t NowNs() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline double Percentile(std::vector<std::uint64_t> values, double p) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  const double clamped = std::clamp(p, 0.0, 1.0);
  const double rank = clamped * static_cast<double>(values.size() - 1);
  const std::size_t lower = static_cast<std::size_t>(std::floor(rank));
  const std::size_t upper = static_cast<std::size_t>(std::ceil(rank));
  if (lower == upper) {
    return static_cast<double>(values[lower]);
  }
  const double weight = rank - static_cast<double>(lower);
  return static_cast<double>(values[lower]) * (1.0 - weight) +
         static_cast<double>(values[upper]) * weight;
}

constexpr double kTailPercentile = 0.99;

inline double FairnessRatio(const std::vector<std::atomic<std::uint64_t>>& counts) {
  if (counts.empty()) {
    return 1.0;
  }
  std::uint64_t min_value = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t max_value = 0;
  for (const auto& value : counts) {
    const std::uint64_t loaded = value.load(std::memory_order_relaxed);
    min_value = std::min(min_value, loaded);
    max_value = std::max(max_value, loaded);
  }
  if (max_value == 0 || min_value == std::numeric_limits<std::uint64_t>::max()) {
    return 1.0;
  }
  return static_cast<double>(min_value) / static_cast<double>(max_value);
}

inline void CpuPause() {
#if defined(_M_X64) || defined(_M_IX86) || defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#else
  std::this_thread::yield();
#endif
}

inline void PreciseDelayUs(std::uint32_t delay_us) {
  if (delay_us == 0) {
    return;
  }
  if (delay_us >= 1000) {
    std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    return;
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(delay_us);
  while (std::chrono::steady_clock::now() < deadline) {
    CpuPause();
  }
}

inline void SpinWaitBackoff(std::uint64_t attempt) {
  if (attempt < 64) {
    std::this_thread::yield();
    return;
  }
  if (attempt < 4096) {
    PreciseDelayUs(1);
    return;
  }
  PreciseDelayUs(10);
}

template<typename Adapter>
inline bool ConsumerShouldStop(
    const ScenarioParams& params,
    const Adapter& adapter,
    std::atomic<int>& producers_done,
    std::atomic<std::uint64_t>& produced,
    std::atomic<std::uint64_t>& delivered,
    std::atomic<bool>& stop_requested) {
  if (stop_requested.load(std::memory_order_acquire)) {
    return true;
  }
  if (producers_done.load(std::memory_order_acquire) != params.producers) {
    return false;
  }
  if (adapter.Size() == 0) {
    return true;
  }
  const std::uint64_t produced_count = produced.load(std::memory_order_relaxed);
  const std::uint64_t delivered_count = delivered.load(std::memory_order_relaxed);
  return produced_count > 0 && delivered_count >= produced_count;
}

inline void ProfileProducerBackoff(
    LoadProfile profile,
    std::uint64_t iteration,
    std::minstd_rand& rng) {
  switch (profile) {
    case LoadProfile::kFastConsumer:
      PreciseDelayUs(2);
      break;
    case LoadProfile::kBatch:
      if (iteration % 256 == 0) {
        PreciseDelayUs(50);
      }
      break;
    case LoadProfile::kBurst:
      if (rng() % 100 < 5) {
        PreciseDelayUs(70);
      }
      break;
    default:
      break;
  }
}

inline void ProfileConsumerBackoff(LoadProfile profile, std::minstd_rand& rng) {
  switch (profile) {
    case LoadProfile::kFastProducer:
      PreciseDelayUs(2);
      break;
    case LoadProfile::kBurst:
      if (rng() % 100 < 5) {
        PreciseDelayUs(70);
      }
      break;
    default:
      break;
  }
}

template<typename Adapter>
ScenarioMetrics RunScenario(const ScenarioParams& params) {
  const std::uint64_t latency_sample_stride = static_cast<std::uint64_t>(
      std::max(1, EnvRead("MQ_BENCH_LATENCY_STRIDE", 1)));
  Adapter adapter(params.capacity);
  const auto start = std::chrono::steady_clock::now();
  const auto deadline = start + std::chrono::seconds(params.scenario_timeout_sec);

  std::atomic<std::uint64_t> next_seq{0};
  std::atomic<std::uint64_t> produced{0};
  std::atomic<std::uint64_t> delivered{0};
  std::atomic<std::uint64_t> blocked_send_attempts{0};
  std::atomic<std::uint64_t> blocked_read_attempts{0};
  std::atomic<int> producers_done{0};
  std::atomic<bool> stop_requested{false};
  std::atomic<bool> scenario_done{false};

  std::thread watchdog([&] {
    while (!scenario_done.load(std::memory_order_acquire)) {
      if (std::chrono::steady_clock::now() >= deadline) {
        stop_requested.store(true, std::memory_order_release);
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  std::vector<std::atomic<std::uint64_t>> producer_counts(
      static_cast<std::size_t>(params.producers));
  std::vector<std::atomic<std::uint64_t>> consumer_counts(
      static_cast<std::size_t>(params.consumers));
  for (auto& c : producer_counts) {
    c.store(0, std::memory_order_relaxed);
  }
  for (auto& c : consumer_counts) {
    c.store(0, std::memory_order_relaxed);
  }

  std::vector<std::uint64_t> send_samples;
  std::vector<std::uint64_t> read_samples;
  const std::size_t sampled_capacity =
      static_cast<std::size_t>(params.total_messages / static_cast<int>(latency_sample_stride) + 1);
  send_samples.reserve(sampled_capacity);
  read_samples.reserve(sampled_capacity);
  std::vector<std::vector<std::uint64_t>> send_samples_by_producer(
      static_cast<std::size_t>(params.producers));
  std::vector<std::vector<std::uint64_t>> read_samples_by_consumer(
      static_cast<std::size_t>(params.consumers));

  std::vector<std::thread> producers;
  std::vector<std::thread> consumers;
  producers.reserve(static_cast<std::size_t>(params.producers));
  consumers.reserve(static_cast<std::size_t>(params.consumers));

  for (int producer_idx = 0; producer_idx < params.producers; ++producer_idx) {
    producers.emplace_back([&, producer_idx] {
      auto& local_send_samples = send_samples_by_producer[static_cast<std::size_t>(producer_idx)];
      std::minstd_rand rng(static_cast<unsigned>(producer_idx + 11));
      const int base = params.total_messages / params.producers;
      const int extra = (producer_idx < (params.total_messages % params.producers)) ? 1 : 0;
      const int local_total = base + extra;
      local_send_samples.reserve(static_cast<std::size_t>(
          local_total / static_cast<int>(latency_sample_stride) + 1));
      for (int i = 0; i < local_total; ++i) {
        if (stop_requested.load(std::memory_order_acquire)) {
          break;
        }
        const std::uint64_t sequence = next_seq.fetch_add(1, std::memory_order_relaxed);
        BenchmarkMessage msg;
        msg.seq = sequence;
        msg.producer_id = static_cast<std::uint64_t>(producer_idx);
        std::uint64_t send_attempts = 0;
        while (true) {
          if (stop_requested.load(std::memory_order_acquire)) {
            break;
          }
          msg.send_ns = NowNs();
          if (adapter.TrySend(msg)) {
            break;
          }
          blocked_send_attempts.fetch_add(1, std::memory_order_relaxed);
          SpinWaitBackoff(send_attempts++);
        }
        if (stop_requested.load(std::memory_order_acquire)) {
          break;
        }
        const std::uint64_t send_end = NowNs();
        producer_counts[static_cast<std::size_t>(producer_idx)].fetch_add(
            1, std::memory_order_relaxed);
        produced.fetch_add(1, std::memory_order_relaxed);
        if ((sequence % latency_sample_stride) == 0 && send_end >= msg.send_ns) {
          local_send_samples.push_back(std::max<std::uint64_t>(1, send_end - msg.send_ns));
        }
        if (!params.no_backoff) {
          ProfileProducerBackoff(params.profile, static_cast<std::uint64_t>(i), rng);
        }
      }
      producers_done.fetch_add(1, std::memory_order_release);
    });
  }

  for (int consumer_idx = 0; consumer_idx < params.consumers; ++consumer_idx) {
    consumers.emplace_back([&, consumer_idx] {
      auto& local_read_samples = read_samples_by_consumer[static_cast<std::size_t>(consumer_idx)];
      std::minstd_rand rng(static_cast<unsigned>(consumer_idx + 101));
      std::uint64_t read_attempts = 0;
      local_read_samples.reserve(static_cast<std::size_t>(
          std::max(1, params.total_messages / params.consumers) / static_cast<int>(latency_sample_stride) + 1));
      while (true) {
        if (ConsumerShouldStop(params, adapter, producers_done, produced, delivered, stop_requested)) {
          break;
        }
        const auto message = adapter.TryRead();
        if (message.has_value()) {
          const std::uint64_t now = NowNs();
          delivered.fetch_add(1, std::memory_order_relaxed);
          consumer_counts[static_cast<std::size_t>(consumer_idx)].fetch_add(
              1, std::memory_order_relaxed);
          if ((message->seq % latency_sample_stride) == 0 && now >= message->send_ns) {
            local_read_samples.push_back(std::max<std::uint64_t>(1, now - message->send_ns));
          }
          if (!params.no_backoff) {
            ProfileConsumerBackoff(params.profile, rng);
          }
          read_attempts = 0;
          continue;
        }
        blocked_read_attempts.fetch_add(1, std::memory_order_relaxed);
        SpinWaitBackoff(read_attempts++);
      }
    });
  }

  for (auto& thread : producers) {
    thread.join();
  }
  for (auto& thread : consumers) {
    thread.join();
  }
  for (auto& sample_set : send_samples_by_producer) {
    send_samples.insert(send_samples.end(), sample_set.begin(), sample_set.end());
  }
  for (auto& sample_set : read_samples_by_consumer) {
    read_samples.insert(read_samples.end(), sample_set.begin(), sample_set.end());
  }

  scenario_done.store(true, std::memory_order_release);
  watchdog.join();

  const auto end = std::chrono::steady_clock::now();
  const double elapsed_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

  ScenarioMetrics metrics;
  metrics.produced = produced.load(std::memory_order_relaxed);
  metrics.delivered = delivered.load(std::memory_order_relaxed);
  metrics.lost = (metrics.produced >= metrics.delivered) ? (metrics.produced - metrics.delivered) : 0;
  metrics.seq_gaps = metrics.lost;
  metrics.blocked_send_attempts = blocked_send_attempts.load(std::memory_order_relaxed);
  metrics.blocked_read_attempts = blocked_read_attempts.load(std::memory_order_relaxed);
  metrics.cas_retries = adapter.CasRetries();
  metrics.elapsed_sec = elapsed_sec;
  if (metrics.elapsed_sec > 0.0) {
    metrics.produce_per_s = static_cast<double>(metrics.produced) / metrics.elapsed_sec;
    metrics.consume_per_s = static_cast<double>(metrics.delivered) / metrics.elapsed_sec;
  }
  metrics.send_p50_ns = Percentile(send_samples, 0.50);
  metrics.send_p99_ns = Percentile(send_samples, kTailPercentile);
  metrics.read_p50_ns = Percentile(read_samples, 0.50);
  metrics.read_p99_ns = Percentile(read_samples, kTailPercentile);
  metrics.producer_fairness = FairnessRatio(producer_counts);
  metrics.consumer_fairness = FairnessRatio(consumer_counts);
  metrics.fairness_ratio = std::min(metrics.producer_fairness, metrics.consumer_fairness);
  metrics.queue_size_at_end = adapter.Size();
  metrics.timed_out = stop_requested.load(std::memory_order_relaxed);
  metrics.producer_threads = static_cast<double>(params.producers);
  metrics.consumer_threads = static_cast<double>(params.consumers);
  metrics.hw_threads = static_cast<double>(params.hw_threads);
  metrics.thread_ratio = params.thread_ratio;
  return metrics;
}

inline void PublishCounters(benchmark::State& state, const ScenarioMetrics& metrics) {
  state.SetItemsProcessed(static_cast<int64_t>(metrics.delivered));
  state.counters["msg_per_s"] = benchmark::Counter(metrics.consume_per_s);
  state.counters["produce_per_s"] = benchmark::Counter(metrics.produce_per_s);
  state.counters["consume_per_s"] = benchmark::Counter(metrics.consume_per_s);
  state.counters["send_p50_ns"] = metrics.send_p50_ns;
  state.counters["send_p99_ns"] = metrics.send_p99_ns;
  state.counters["read_p50_ns"] = metrics.read_p50_ns;
  state.counters["read_p99_ns"] = metrics.read_p99_ns;
  state.counters["lost_messages"] = static_cast<double>(metrics.lost);
  state.counters["seq_gaps"] = static_cast<double>(metrics.seq_gaps);
  state.counters["producer_fairness"] = metrics.producer_fairness;
  state.counters["consumer_fairness"] = metrics.consumer_fairness;
  state.counters["fairness_ratio"] = metrics.fairness_ratio;
  state.counters["blocked_send_attempts"] = static_cast<double>(metrics.blocked_send_attempts);
  state.counters["blocked_read_attempts"] = static_cast<double>(metrics.blocked_read_attempts);
  state.counters["cas_retries"] = static_cast<double>(metrics.cas_retries);
  state.counters["timed_out"] = metrics.timed_out ? 1.0 : 0.0;
  state.counters["queue_size_at_end"] = static_cast<double>(metrics.queue_size_at_end);
  state.counters["producer_threads"] = metrics.producer_threads;
  state.counters["consumer_threads"] = metrics.consumer_threads;
  state.counters["hw_threads"] = metrics.hw_threads;
  state.counters["thread_ratio"] = metrics.thread_ratio;
}

} 

