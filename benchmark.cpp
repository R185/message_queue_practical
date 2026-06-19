#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

#include "../lib/circular/michael_scott_queue.hpp"

struct BenchmarkMessage {
    std::uint64_t seq = 0;
    std::uint32_t priority = 0;
    std::uint32_t producer_id = 0;
    bool stop = false;

    static BenchmarkMessage Data(std::uint64_t s, std::uint32_t p, std::uint32_t pid) {
        return {s, p, pid, false};
    }

    static BenchmarkMessage Stop() {
        return {0, std::numeric_limits<std::uint32_t>::max(),
                std::numeric_limits<std::uint32_t>::max(), true};
    }

    bool IsStop() const { return stop; }
};


template <typename Queue, typename SendFn, typename ReadFn>
void Bench(const char* name,
           Queue& queue,
           int producers,
           int consumers,
           std::size_t messages_per_producer,
           SendFn send_fn,
           ReadFn read_fn) {

    std::atomic<int> ready_count{0};
    std::atomic<bool> go{false};
    std::atomic<int> producers_left{producers};

    std::vector<std::size_t> recv_count(consumers, 0);
    std::vector<std::thread> threads;
    threads.reserve(producers + consumers);

    for (int c = 0; c < consumers; c++) {
        threads.emplace_back([&, c]() {
            ready_count.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            while (true) {
                BenchmarkMessage msg = read_fn(queue);
                if (msg.IsStop()) break;
                recv_count[c]++;
            }
        });
    }

    for (int p = 0; p < producers; p++) {
        threads.emplace_back([&, p]() {
            ready_count.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (std::size_t i = 0; i < messages_per_producer; i++) {
                send_fn(queue, BenchmarkMessage::Data(
                    static_cast<std::uint64_t>(p) * messages_per_producer + i,
                    static_cast<std::uint32_t>(i & 31),
                    static_cast<std::uint32_t>(p)));
            }

            if (producers_left.fetch_sub(1) == 1) {
                for (int i = 0; i < consumers; i++) {
                    send_fn(queue, BenchmarkMessage::Stop());
                }
            }
        });
    }

    while (ready_count.load() != producers +consumers) {
        std::this_thread::yield();
    }

    auto t0 = std::chrono::steady_clock::now();
    go.store(true,std::memory_order_release);

    for (auto& t : threads) t.join();

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();

    std::size_t sent = static_cast<std::size_t>(producers) * messages_per_producer;
    std::size_t received = 0;
    for (auto x : recv_count) received += x;

    std::cout << name
              << " | P=" << producers
              << " C=" << consumers
              << " | sent=" << sent
              << " rcvd=" << received
              << " | " << sec << " sec"
              << " | send " << (sent / sec / 1e6) << " Mops/s"
              << " | rcvd " << (received / sec / 1e6) << " Mops/s"
              << "\n" << (sent == received ? "OK" : "DROPPED")
              << '\n';
}


// Для IMessageQueue
struct BlockingSend {
    template <typename Q>
    void operator()(Q& q, BenchmarkMessage msg) const {
        q.Send(std::move(msg));
    }
};

struct BlockingRead {
    template <typename Q>
    BenchmarkMessage operator()(Q& q) const {
        return q.Read();
    }
};

// Для Queue с TrySend/TryRead
struct TrySpinSend {
    template <typename Q>
    void operator()(Q& q, BenchmarkMessage msg) const {
        while (!q.TrySend(msg)) {
            std::this_thread::yield();
        }
    }
};

struct TrySpinRead {
    template <typename Q>
    BenchmarkMessage operator()(Q& q) const {
        while (true) {
            auto msg = q.TryRead();
            if (msg) return std::move(*msg);
            std::this_thread::yield();
        }
    }
};

// Для очереди где Send -> bool и Read -> optional
struct BoolSpinSend {
    template <typename Q>
    void operator()(Q& q, BenchmarkMessage msg) const {
        while (!q.Send(std::move(msg))) {
            std::this_thread::yield();
        }
    }
};

struct OptionalSpinRead {
    template <typename Q>
    BenchmarkMessage operator()(Q& q) const {
        while (true) {
            auto msg = q.Read();
            if (msg) return std::move(*msg);
            std::this_thread::yield();
        }
    }
};


// int main() {
//     constexpr std::size_t kCapacity = 1 << 8;
//     constexpr std::size_t kMessages = 1'000'000;
//
//     //Michael-Scott
//     {
//         Queue<BenchmarkMessage> queue(kCapacity);
//         Bench("michael_scott_MPMC", queue, 4, 4, kMessages,
//               BoolSpinSend{}, OptionalSpinRead{});
//     }
//
//     {
//         Queue<BenchmarkMessage> queue(kCapacity);
//         Bench("michael_scott_MPSC", queue, 4, 1, kMessages,
//               BoolSpinSend{}, OptionalSpinRead{});
//     }
//
//     return 0;
// }