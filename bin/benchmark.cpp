#include <algorithm>
#include <array>
#include <cstdlib>
#include <string>

#include <benchmark/benchmark.h>

#include "benchmark_helpers.hpp"
#include "michael_scott_queue.hpp"
#include "min-heap_queue.hpp"

namespace mq = message_queue;
namespace mqb = message_queue::bench;

struct BenchmarkMessageComparator {
  bool operator()(const mqb::BenchmarkMessage& lhs, const mqb::BenchmarkMessage& rhs) const noexcept {
    return lhs.seq < rhs.seq;
  }
};

template<typename T, mq::ThreadAccessCategory Category, mq::DeadlockExceptionPolicy Policy>
using PriorityQueueAlias = mq::PriorityMessageQueue<
    T, Category, Policy, BenchmarkMessageComparator>;

struct CircularBoundedFactory {
  template<typename T, mq::ThreadAccessCategory Cat, mq::DeadlockExceptionPolicy Pol>
  using QueueType = mq::CircularBoundedMessageQueue<T, Cat, Pol>;
};

struct CircularOverwriteFactory {
  template<typename T, mq::ThreadAccessCategory Cat, mq::DeadlockExceptionPolicy Pol>
  using QueueType = mq::CircularDropOldestMessageQueue<T, Cat, Pol>;
};

struct DequeBoundedFactory {
  template<typename T, mq::ThreadAccessCategory Cat, mq::DeadlockExceptionPolicy Pol>
  using QueueType = mq::DequeBoundedMessageQueue<T, Cat, Pol>;
};

struct MichaelScottFactory {
  template<typename T, mq::ThreadAccessCategory Cat, mq::DeadlockExceptionPolicy Pol>
  using QueueType = mq::MichaelScottQueue<T, Cat, Pol>;
};

struct PriorityQueueFactory {
  template<typename T, mq::ThreadAccessCategory Cat, mq::DeadlockExceptionPolicy Pol>
  using QueueType = PriorityQueueAlias<T, Cat, Pol>;
};

int EnvRead(const char* name, int default_value) noexcept {
  if (const char* env = std::getenv(name)) {
    return std::atoi(env);
  }
  return default_value;
}

int BenchmarkRepetitions() noexcept {
  return EnvRead("MQ_BENCH_REPETITIONS", 5);
}

enum class Strategy{
  kCircularBounded,
  kCircularOverwrite,
  kDequeBounded,
  kMichaelScott,
  kPriorityStub
};

template<typename Factory, mq::ThreadAccessCategory Category>
mqb::ScenarioMetrics RunTopologyScenario(const mqb::ScenarioParams& params) {
  using QueueType = typename Factory::template QueueType<
      mqb::BenchmarkMessage,
      Category,
      mq::DeadlockExceptionPolicy::kNoException>;
  using Adapter = mqb::IMessageQueueAdapter<QueueType>;
  return mqb::RunScenario<Adapter>(params);
}

template<typename Factory>
mqb::ScenarioMetrics RunIMessageQueueForTopology(const mqb::ScenarioParams& params, mqb::Topology topology) {
  switch (topology) {
    case mqb::Topology::kSpsc:
      return RunTopologyScenario<Factory, mq::ThreadAccessCategory::kSingleProducerSingleConsumer>(params);
    case mqb::Topology::kMpsc:
      return RunTopologyScenario<Factory, mq::ThreadAccessCategory::kMultipleProducerSingleConsumer>(params);
    case mqb::Topology::kSpmc:
      return RunTopologyScenario<Factory, mq::ThreadAccessCategory::kSingleProducerMultipleConsumer>(params);
    case mqb::Topology::kMpmc:
      return RunTopologyScenario<Factory, mq::ThreadAccessCategory::kMultipleProducerMultipleConsumer>(params);
  }
  return RunTopologyScenario<Factory, mq::ThreadAccessCategory::kSingleProducerSingleConsumer>(params);
}

mqb::ScenarioMetrics RunStrategy(
    Strategy strategy,
    const mqb::ScenarioParams& params,
    mqb::Topology topology) {
  switch (strategy) {
    case Strategy::kCircularBounded:
      return RunIMessageQueueForTopology<CircularBoundedFactory>(params, topology);
    case Strategy::kCircularOverwrite:
      return RunIMessageQueueForTopology<CircularOverwriteFactory>(params, topology);
    case Strategy::kDequeBounded:
      return RunIMessageQueueForTopology<DequeBoundedFactory>(params, topology);
    case Strategy::kMichaelScott:
      return RunIMessageQueueForTopology<MichaelScottFactory>(params, topology);
    case Strategy::kPriorityStub:
      return RunIMessageQueueForTopology<PriorityQueueFactory>(params, topology);
  }
  return {};
}

std::string StrategyName(Strategy strategy) {
  switch (strategy) {
    case Strategy::kCircularBounded:
      return "circular_bounded";
    case Strategy::kCircularOverwrite:
      return "circular_overwrite";
    case Strategy::kDequeBounded:
      return "deque_bounded";
    case Strategy::kMichaelScott:
      return "michael_scott";
    case Strategy::kPriorityStub:
      return "priority_queue";
  }
  return "unknown";
}

std::string ProfileName(mqb::LoadProfile profile) {
  switch (profile) {
    case mqb::LoadProfile::kBalanced:
      return "balanced";
    case mqb::LoadProfile::kFastProducer:
      return "fast_producer";
    case mqb::LoadProfile::kFastConsumer:
      return "fast_consumer";
    case mqb::LoadProfile::kBatch:
      return "batch";
    case mqb::LoadProfile::kBurst:
      return "burst";
    case mqb::LoadProfile::kPriorityMix:
      return "priority_mix";
  }
  return "unknown";
}

std::string TopologyName(mqb::Topology topology) {
  switch (topology) {
    case mqb::Topology::kSpsc:
      return "P1C1";
    case mqb::Topology::kMpsc:
      return "P4C1";
    case mqb::Topology::kSpmc:
      return "P1C4";
    case mqb::Topology::kMpmc:
      return "P4C4";
  }
  return "P1C1";
}

void BM_QueueScenario(::benchmark::State& state) {
  const auto strategy = static_cast<Strategy>(state.range(0));
  const auto profile = static_cast<mqb::LoadProfile>(state.range(1));
  const auto topology = static_cast<mqb::Topology>(state.range(2));
  const std::size_t capacity = static_cast<std::size_t>(state.range(3));
  const int total_messages = static_cast<int>(state.range(4));
  const mqb::ThreadLayout layout = mqb::ThreadsForTopology(topology);

  mqb::ScenarioMetrics last_run{};
  for (auto _ : state) {
    (void)_;
    mqb::ScenarioParams params;
    const int capacity_override = EnvRead("MQ_BENCH_CAPACITY", -1);
    params.capacity = capacity_override > 0 ? static_cast<std::size_t>(capacity_override) : capacity;
    params.producers = layout.producers;
    params.consumers = layout.consumers;
    params.hw_threads = layout.hw_threads;
    params.thread_ratio = layout.thread_ratio;
    const int message_override = EnvRead("MQ_BENCH_MESSAGES", -1);
    params.total_messages = message_override > 0 ? message_override : total_messages;
    params.profile = profile;
    params.scenario_timeout_sec = std::max(1, EnvRead("MQ_BENCH_TIMEOUT_SEC", 120));
    params.no_backoff = mqb::EnvFlag("MQ_BENCH_NO_BACKOFF", false);
    
    const mqb::ScenarioMetrics run = RunStrategy(strategy, params, topology);
    state.SetIterationTime(run.elapsed_sec);
    if (run.timed_out) {
      state.SkipWithError("Scenario timed out: possible deadlock or livelock");
      return;
    }
    last_run = run;
  }
  mqb::PublishCounters(state, last_run);
}

void ApplyCase5Args(::benchmark::internal::Benchmark* benchmark_case) {
  constexpr std::array<int, 5> strategies = {
      static_cast<int>(Strategy::kCircularBounded),
      static_cast<int>(Strategy::kCircularOverwrite),
      static_cast<int>(Strategy::kDequeBounded),
      static_cast<int>(Strategy::kMichaelScott),
      static_cast<int>(Strategy::kPriorityStub)};
  constexpr std::array<int, 6> profiles = {
      static_cast<int>(mqb::LoadProfile::kBalanced),
      static_cast<int>(mqb::LoadProfile::kFastProducer),
      static_cast<int>(mqb::LoadProfile::kFastConsumer),
      static_cast<int>(mqb::LoadProfile::kBatch),
      static_cast<int>(mqb::LoadProfile::kBurst),
      static_cast<int>(mqb::LoadProfile::kPriorityMix)};
  constexpr std::array<int, 4> topologies = {
      static_cast<int>(mqb::Topology::kSpsc),
      static_cast<int>(mqb::Topology::kMpsc),
      static_cast<int>(mqb::Topology::kSpmc),
      static_cast<int>(mqb::Topology::kMpmc)};
  constexpr std::array<int, 3> capacities = {64, 1024, 4096};
  constexpr std::array<int, 1> messages = {100000};

  for (const int strategy : strategies) {
    for (const int profile : profiles) {
      for (const int topology : topologies) {
        for (const int capacity : capacities) {
          for (const int total : messages) {
            benchmark_case->Args({strategy, profile, topology, capacity, total});
          }
        }
      }
    }
  }
}

BENCHMARK(BM_QueueScenario)
    ->Apply(ApplyCase5Args)
    ->UseManualTime()
    ->Iterations(1)
    ->Repetitions(BenchmarkRepetitions())
    ->Unit(::benchmark::kMillisecond);

BENCHMARK_MAIN();