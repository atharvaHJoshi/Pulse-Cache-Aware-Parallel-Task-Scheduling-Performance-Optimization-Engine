#pragma once

#include "pulse/task.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <vector>

namespace pulse {

/// Timing for one executed task, all relative to the start of the run.
struct TaskRecord {
    TaskId id{0};
    WorkerId worker{0};
    std::uint64_t ready_ns{0};
    std::uint64_t start_ns{0};
    std::uint64_t end_ns{0};

    std::uint64_t wait_ns() const noexcept { return start_ns - ready_ns; }
    std::uint64_t run_ns() const noexcept { return end_ns - start_ns; }
    std::uint64_t latency_ns() const noexcept { return end_ns - ready_ns; }
};

/// Aggregate output produced by a single run of a scheduling policy.
struct RunResult {
    std::string policy;
    std::size_t workers{0};
    std::size_t tasks{0};

    std::uint64_t makespan_ns{0};   // wall time from first start to last completion
    std::uint64_t total_work_ns{0}; // sum of per-task execution times

    std::vector<TaskRecord> records;
    std::vector<std::uint64_t> worker_busy_ns;  // per-worker execution time
    std::vector<std::uint64_t> worker_idle_ns;  // per-worker waiting time
    std::size_t steals{0};                      // successful work-stealing operations
    std::size_t conflict_migrations{0};         // tasks dispatched away from their locality home to balance load

    double throughput() const noexcept {
        if (makespan_ns == 0) return 0.0;
        return static_cast<double>(tasks) * 1e9 / static_cast<double>(makespan_ns);
    }

    double speedup(std::size_t /*baseline_workers*/ = 1) const noexcept {
        if (makespan_ns == 0) return 0.0;
        return static_cast<double>(total_work_ns) / static_cast<double>(makespan_ns);
    }

    /// Max busy/wall ratio: 1.0 is perfectly balanced, 1/N is one busy core.
    double utilization() const noexcept {
        if (makespan_ns == 0 || workers == 0) return 0.0;
        return static_cast<double>(total_work_ns) /
               (static_cast<double>(makespan_ns) * static_cast<double>(workers));
    }

    /// std-dev of per-worker busy time / mean busy time. Lower is more balanced.
    double load_imbalance() const noexcept {
        if (worker_busy_ns.empty()) return 0.0;
        double mean = static_cast<double>(total_work_ns) /
                      static_cast<double>(worker_busy_ns.size());
        if (mean <= 0.0) return 0.0;
        double var = 0.0;
        for (auto b : worker_busy_ns) {
            double d = static_cast<double>(b) - mean;
            var += d * d;
        }
        var /= static_cast<double>(worker_busy_ns.size());
        return std::sqrt(var) / mean;
    }

    static std::uint64_t percentile(std::vector<std::uint64_t> values, double p) {
        if (values.empty()) return 0;
        std::sort(values.begin(), values.end());
        double idx = p / 100.0 * static_cast<double>(values.size() - 1);
        std::size_t lo = static_cast<std::size_t>(std::floor(idx));
        std::size_t hi = static_cast<std::size_t>(std::ceil(idx));
        if (lo == hi) return values[lo];
        double frac = idx - static_cast<double>(lo);
        return static_cast<std::uint64_t>(
            static_cast<double>(values[lo]) * (1.0 - frac) + static_cast<double>(values[hi]) * frac);
    }

    std::uint64_t p50_latency() const {
        std::vector<std::uint64_t> v;
        v.reserve(records.size());
        for (auto& r : records) v.push_back(r.latency_ns());
        return percentile(std::move(v), 50.0);
    }
    std::uint64_t p95_latency() const {
        std::vector<std::uint64_t> v;
        v.reserve(records.size());
        for (auto& r : records) v.push_back(r.latency_ns());
        return percentile(std::move(v), 95.0);
    }
    std::uint64_t p99_latency() const {
        std::vector<std::uint64_t> v;
        v.reserve(records.size());
        for (auto& r : records) v.push_back(r.latency_ns());
        return percentile(std::move(v), 99.0);
    }
    std::uint64_t p99_wait() const {
        std::vector<std::uint64_t> v;
        v.reserve(records.size());
        for (auto& r : records) v.push_back(r.wait_ns());
        return percentile(std::move(v), 99.0);
    }
};

} // namespace pulse
