#pragma once

#include "pulse/dag.hpp"
#include "pulse/policy.hpp"
#include "pulse/stats.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace pulse {

/// Executes a DAG across a fixed pool of worker threads using a pluggable
/// SchedulingPolicy.
///
/// Responsibilities:
///   * own worker threads and the lifecycle of a run
///   * resolve dependencies (the only place dep_count is decremented)
///   * wake sleeping workers without a global lock on the hot path
///   * collect per-task and per-worker timings
///
/// A Runtime instance is reusable: run() resets per-run state, so a benchmark
/// can run many iterations without reallocating threads.
class Runtime {
public:
    Runtime(std::size_t workers, std::shared_ptr<SchedulingPolicy> policy);
    ~Runtime();

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    /// Execute `dag`.  The graph must already be finalized.  Blocking.
    RunResult run(DAG& dag);

    std::size_t workers() const noexcept { return workers_; }
    const SchedulingPolicy& policy() const noexcept { return *policy_; }

private:
    void worker_loop(WorkerId id);
    void execute(WorkerId id, const TaskHandle& task);
    void make_ready(const TaskHandle& task, WorkerId producer);
    static std::uint64_t now_ns();

    std::size_t workers_;
    std::shared_ptr<SchedulingPolicy> policy_;

    // per-run state
    DAG* dag_{nullptr};
    std::atomic<std::size_t> completed_{0};
    std::size_t total_{0};

    std::mutex wake_mtx_;
    std::condition_variable wake_cv_;
    // Counting wakeups: one token per newly-ready task.  A sleeping worker
    // consumes one token per attempt, which bounds spinning without losing
    // wakeups (the classic counting-semaphore pattern).
    std::atomic<std::size_t> wakeups_{0};
    std::atomic<bool> cancel_{false};

    std::vector<std::vector<TaskRecord>> records_;
    std::vector<std::uint64_t> worker_busy_;
    std::vector<std::uint64_t> worker_idle_;

    std::vector<std::thread> threads_;
};

/// Policy factories (one translation unit each).
std::shared_ptr<SchedulingPolicy> make_fifo_policy();
std::shared_ptr<SchedulingPolicy> make_work_stealing_policy();
std::shared_ptr<SchedulingPolicy> make_critical_path_policy();
std::shared_ptr<SchedulingPolicy> make_locality_aware_policy();

} // namespace pulse
