#include "pulse/runtime.hpp"

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <vector>

namespace pulse {
namespace {

/// Chase-Lev style work stealing with per-worker deques.
///
/// A finished task's children are pushed onto the *producer's* deque, so a
/// child tends to run on the core that just touched its inputs (locality by
/// default).  When a worker runs dry it steals the oldest task from a random
/// victim, which keeps long dependency chains moving without a global lock.
class WorkStealingPolicy final : public SchedulingPolicy {
public:
    void on_dag_ready(const DAG&, std::size_t workers) override {
        workers_ = workers == 0 ? 1 : workers;
        queues_.clear();
        locks_.clear();
        queues_.resize(workers_);
        rngs_.clear();
        rngs_.resize(workers_);
        for (std::size_t i = 0; i < workers_; ++i) {
            locks_.push_back(std::make_unique<std::mutex>());
            // Distinct seed per worker: try_acquire() runs concurrently on every
            // worker, so a shared RNG would be a data race.
            rngs_[i].seed(0x9e3779b97f4a7c15ULL + i * 0x100000001b3ULL);
        }
    }

    void on_ready(const TaskHandle& task, WorkerId producer) override {
        WorkerId dst = producer % workers_;
        std::lock_guard<std::mutex> lk(*locks_[dst]);
        queues_[dst].push_back(task);
    }

    TaskHandle try_acquire(WorkerId worker) override {
        // Fast path: own deque (LIFO keeps the working set hot).
        {
            std::lock_guard<std::mutex> lk(*locks_[worker]);
            if (!queues_[worker].empty()) {
                TaskHandle t = queues_[worker].back();
                queues_[worker].pop_back();
                return t;
            }
        }
        if (workers_ < 2) return nullptr;

        // Steal from victims (FIFO: take the oldest, biggest-granularity work).
        WorkerId start = rngs_[worker]() % workers_;
        for (std::size_t k = 0; k < workers_; ++k) {
            WorkerId v = (start + k) % workers_;
            if (v == worker) continue;
            std::lock_guard<std::mutex> lk(*locks_[v]);
            if (!queues_[v].empty()) {
                TaskHandle t = queues_[v].front();
                queues_[v].pop_front();
                steals_.fetch_add(1, std::memory_order_relaxed);
                return t;
            }
        }
        return nullptr;
    }

    std::size_t size() const noexcept override {
        std::size_t n = 0;
        for (std::size_t i = 0; i < queues_.size(); ++i) {
            std::lock_guard<std::mutex> lk(*locks_[i]);
            n += queues_[i].size();
        }
        return n;
    }

    std::size_t steals() const noexcept override { return steals_.load(std::memory_order_relaxed); }

    const char* name() const noexcept override { return "work-stealing"; }

private:
    std::size_t workers_{1};
    std::vector<std::deque<TaskHandle>> queues_;
    std::vector<std::unique_ptr<std::mutex>> locks_;
    std::atomic<std::size_t> steals_{0};
    std::vector<std::mt19937_64> rngs_; // one per worker: no sharing on the steal path
};

} // namespace

std::shared_ptr<SchedulingPolicy> make_work_stealing_policy() {
    return std::make_shared<WorkStealingPolicy>();
}

} // namespace pulse
