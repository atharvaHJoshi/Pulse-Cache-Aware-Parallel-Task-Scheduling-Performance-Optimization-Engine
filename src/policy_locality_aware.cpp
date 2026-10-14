#include "pulse/per_worker_queues.hpp"
#include "pulse/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

namespace pulse {
namespace {

/// Locality-aware adaptive scheduler -- the experimental contribution.
///
/// It starts from the critical-path scheduler's distributed priority queues (so
/// it gets bottom-level ordering for free) and adds two adaptive mechanisms:
///
///   1. *Measured data locality.*  Tasks declare the data regions they touch.
///      Every time a task finishes, the region's "last toucher" is updated to
///      the worker that actually ran it.  A newly ready task is then dispatched
///      to the core that most recently touched its data, so the region's cache
///      lines stay resident in that core's private cache instead of bouncing
///      across the coherence fabric.  Before a region has ever been touched we
///      fall back to a static, load-balanced partition of regions onto workers.
///
///   2. *Load-aware override.*  Locality is only worth preserving while the
///      local worker is not the bottleneck.  If the preferred worker's load
///      (queued + running tasks) exceeds the least-loaded worker's by more than
///      `slack`, the task is migrated to the idle core instead.  `slack` is not
///      a constant: it is derived from how much of the workload actually shares
///      data.  Graph shapes with no data accesses get slack 1 (behave like a
///      balanced scheduler); memory-bound graphs get a larger slack and keep
///      more cache reuse.  Migrations are counted so the trade-off is visible
///      in the benchmark output.
///
/// The hypothesis under test is that (1) wins on memory-bound graphs while (2)
/// prevents (1) from causing tail-latency regressions on irregular graphs.
class LocalityAwarePolicy final : public SchedulingPolicy {
public:
    void on_dag_ready(const DAG& dag, std::size_t workers) override {
        q_.init(workers);
        const std::size_t W = q_.workers();

        running_ = std::make_unique<std::atomic<int>[]>(W);
        for (std::size_t i = 0; i < W; ++i) {
            running_[i].store(0, std::memory_order_relaxed);
        }
        migrations_.store(0, std::memory_order_relaxed);
        started_.store(false, std::memory_order_relaxed);

        // Region statistics: how much work each region carries, and how much of
        // the whole graph is locality-relevant at all.
        DataId max_id = 0;
        std::size_t with_data = 0;
        for (const auto& t : dag.tasks()) {
            if (!t->data_accesses.empty()) ++with_data;
            for (const auto& d : t->data_accesses) max_id = std::max(max_id, d.id);
        }
        std::vector<std::uint64_t> region_load(max_id + 1, 0);
        for (const auto& t : dag.tasks()) {
            for (const auto& d : t->data_accesses) {
                if (d.id < region_load.size()) region_load[d.id] += t->cost_ns;
            }
        }

        // Static fallback: partition regions across workers greedily by total
        // cost so the initial frontier is balanced.  This is used only until a
        // region has actually been touched.
        region_owner_.assign(max_id + 1, kUnknown);
        std::vector<std::uint64_t> worker_bin(W, 0);
        std::vector<DataId> order;
        for (DataId id = 0; id < region_load.size(); ++id) {
            if (region_load[id] > 0) order.push_back(id);
        }
        std::sort(order.begin(), order.end(), [&](DataId a, DataId b) {
            return region_load[a] > region_load[b];
        });
        for (DataId id : order) {
            std::size_t best = 0;
            for (std::size_t w = 1; w < W; ++w) {
                if (worker_bin[w] < worker_bin[best]) best = w;
            }
            region_owner_[id] = best;
            worker_bin[best] += region_load[id];
        }

        // Measured owner: the worker that last touched each region.  Updated as
        // tasks finish, so it follows the actual access pattern rather than the
        // static guess.
        region_count_ = max_id + 1;
        region_last_ = std::make_unique<std::atomic<WorkerId>[]>(region_count_);
        for (std::size_t i = 0; i < region_count_; ++i) {
            region_last_[i].store(kUnknown, std::memory_order_relaxed);
        }

        // Adaptiveness knob: tolerate more imbalance (keep more locality) as the
        // fraction of data-sharing tasks grows.  A graph with no data accesses
        // stays at the minimum slack 1 (never worth churning queues for a
        // locality that does not exist), while a fully data-sharing graph gets
        // the maximum slack 6.  This trades cache reuse against a shorter tail.
        const std::size_t total = dag.size();
        const double strength =
            total ? static_cast<double>(with_data) / static_cast<double>(total) : 0.0;
        slack_ = 1 + static_cast<int>(strength * 5.0 + 0.5);
    }

    void on_ready(const TaskHandle& task, WorkerId producer) override {
        // Serialized by the runtime wake mutex, so choosing a target and
        // inserting is atomic with respect to other on_ready() calls.
        const std::size_t W = q_.workers();
        const bool live = started_.load(std::memory_order_relaxed);

        if (!task->data_accesses.empty()) {
            // A locality home exists: honor it while the source frontier is
            // still being published (queue depth there reflects enqueue order,
            // not real load), then let the load override act during execution.
            WorkerId home = region_home(task);
            if (home == kUnknown) home = producer % W;
            const WorkerId target = live ? choose_target(home) : home;
            if (target != home) migrations_.fetch_add(1, std::memory_order_relaxed);
            q_.insert(target, task);
            return;
        }

        // No data to keep resident.  Before execution starts, producer is
        // meaningless (all sources report producer 0), so just balance the
        // frontier; afterwards keep the task near its producer unless that
        // worker is overloaded.
        if (live) {
            const WorkerId home = producer % W;
            const WorkerId target = choose_target(home);
            if (target != home) migrations_.fetch_add(1, std::memory_order_relaxed);
            q_.insert(target, task);
        } else {
            q_.insert(least_loaded(), task);
        }
    }

    TaskHandle try_acquire(WorkerId worker) override {
        TaskHandle t = q_.take_own(worker);
        return t ? t : q_.steal(worker);
    }

    void on_start(WorkerId worker, const TaskHandle& /*task*/) override {
        started_.store(true, std::memory_order_relaxed);
        running_[worker].fetch_add(1, std::memory_order_relaxed);
    }

    void on_finish(WorkerId worker, const TaskHandle& task) override {
        running_[worker].fetch_sub(1, std::memory_order_relaxed);
        for (const auto& d : task->data_accesses) {
            if (d.id < region_count_) {
                region_last_[d.id].store(worker, std::memory_order_relaxed);
            }
        }
    }

    std::size_t size() const noexcept override { return q_.size(); }
    std::size_t steals() const noexcept override { return q_.steals(); }
    std::size_t migrations() const noexcept override {
        return migrations_.load(std::memory_order_relaxed);
    }
    const char* name() const noexcept override { return "locality-aware"; }

private:
    static constexpr WorkerId kUnknown = static_cast<WorkerId>(-1);

    /// Locality home for a task: the core that last touched its data, else the
    /// static partition.  kUnknown if the task declares no region we know.
    WorkerId region_home(const TaskHandle& task) const {
        for (const auto& d : task->data_accesses) {
            if (region_last_ && d.id < region_count_) {
                const WorkerId last = region_last_[d.id].load(std::memory_order_relaxed);
                if (last != kUnknown) return last;
            }
        }
        for (const auto& d : task->data_accesses) {
            if (d.id < region_owner_.size() && region_owner_[d.id] != kUnknown) {
                return region_owner_[d.id];
            }
        }
        return kUnknown;
    }

    int load(WorkerId worker) const {
        return q_.size_of(worker) + running_[worker].load(std::memory_order_relaxed);
    }

    WorkerId least_loaded() const {
        const std::size_t W = q_.workers();
        WorkerId best = 0;
        int best_load = load(0);
        for (std::size_t w = 1; w < W; ++w) {
            const int l = load(w);
            if (l < best_load) {
                best_load = l;
                best = w;
            }
        }
        return best;
    }

    /// Keep the home worker unless it is overloaded relative to the least
    /// loaded one by more than the adaptively chosen slack.
    WorkerId choose_target(WorkerId home) const {
        if (q_.workers() < 2) return home;
        const WorkerId best = least_loaded();
        if (best == home) return home;
        if (load(home) - load(best) > slack_) return best;
        return home;
    }

    detail::PerWorkerQueues q_;
    std::unique_ptr<std::atomic<int>[]> running_;
    std::vector<WorkerId> region_owner_;         // static fallback partition
    std::unique_ptr<std::atomic<WorkerId>[]> region_last_; // measured last toucher
    std::size_t region_count_{0};
    int slack_{1};
    std::atomic<bool> started_{false};
    std::atomic<std::size_t> migrations_{0};
};

} // namespace

std::shared_ptr<SchedulingPolicy> make_locality_aware_policy() {
    return std::make_shared<LocalityAwarePolicy>();
}

} // namespace pulse
