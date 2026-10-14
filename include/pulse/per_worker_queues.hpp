#pragma once

#include "pulse/task.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

namespace pulse {
namespace detail {

/// A set of per-worker max-heaps (keyed by bottom level) with work stealing.
///
/// This is the shared machinery used by the *distributed* priority schedulers
/// (critical-path and locality-aware).  Sharing it keeps those two policies
/// honest: they differ only in how they choose a target worker, not in the data
/// structure or locking strategy they are measured against.
///
/// Each worker pops its most urgent (highest bottom-level) task first.  An idle
/// worker steals the most urgent task from a peer: taking the highest-priority
/// ready task onto a free core is exactly what minimises makespan in list
/// scheduling, and the victim simply continues with its next task.
class PerWorkerQueues {
public:
    void init(std::size_t workers) {
        workers_ = workers == 0 ? 1 : workers;
        heaps_.clear();
        locks_.clear();
        heaps_.resize(workers_);
        depth_ = std::make_unique<std::atomic<int>[]>(workers_);
        for (std::size_t i = 0; i < workers_; ++i) {
            locks_.push_back(std::make_unique<std::mutex>());
            depth_[i].store(0, std::memory_order_relaxed);
        }
        steals_.store(0, std::memory_order_relaxed);
    }

    std::size_t workers() const noexcept { return workers_; }

    void insert(WorkerId worker, const TaskHandle& task) {
        std::lock_guard<std::mutex> lk(*locks_[worker]);
        heaps_[worker].push(task);
        depth_[worker].fetch_add(1, std::memory_order_relaxed);
    }

    /// Pop the most urgent task owned by `worker`, or nullptr.
    TaskHandle take_own(WorkerId worker) {
        std::lock_guard<std::mutex> lk(*locks_[worker]);
        auto& h = heaps_[worker];
        if (h.empty()) return nullptr;
        TaskHandle t = h.top();
        h.pop();
        depth_[worker].fetch_sub(1, std::memory_order_relaxed);
        return t;
    }

    /// Steal the most urgent task from another worker (round-robin start).
    TaskHandle steal(WorkerId self) {
        if (workers_ < 2) return nullptr;
        for (std::size_t k = 1; k < workers_; ++k) {
            WorkerId v = (self + k) % workers_;
            std::lock_guard<std::mutex> lk(*locks_[v]);
            auto& h = heaps_[v];
            if (!h.empty()) {
                TaskHandle t = h.top();
                h.pop();
                depth_[v].fetch_sub(1, std::memory_order_relaxed);
                steals_.fetch_add(1, std::memory_order_relaxed);
                return t;
            }
        }
        return nullptr;
    }

    std::size_t size() const noexcept {
        std::size_t n = 0;
        for (std::size_t i = 0; i < workers_; ++i) {
            n += static_cast<std::size_t>(depth_[i].load(std::memory_order_relaxed));
        }
        return n;
    }

    /// Queued (not yet running) task count for one worker; lock-free.
    int size_of(WorkerId worker) const noexcept {
        return depth_[worker].load(std::memory_order_relaxed);
    }

    std::size_t steals() const noexcept { return steals_.load(std::memory_order_relaxed); }

private:
    struct Cmp {
        bool operator()(const TaskHandle& a, const TaskHandle& b) const {
            if (a->critical_path_len != b->critical_path_len) {
                return a->critical_path_len < b->critical_path_len; // larger first
            }
            return a->id > b->id;
        }
    };
    using Heap = std::priority_queue<TaskHandle, std::vector<TaskHandle>, Cmp>;

    std::size_t workers_{1};
    std::vector<Heap> heaps_;
    std::vector<std::unique_ptr<std::mutex>> locks_;
    std::unique_ptr<std::atomic<int>[]> depth_;
    std::atomic<std::size_t> steals_{0};
};

} // namespace detail
} // namespace pulse
