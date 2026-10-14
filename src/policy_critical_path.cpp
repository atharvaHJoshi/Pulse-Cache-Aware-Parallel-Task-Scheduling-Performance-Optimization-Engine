#include "pulse/per_worker_queues.hpp"
#include "pulse/runtime.hpp"

namespace pulse {
namespace {

/// Critical-path (bottom-level list) scheduling, distributed.
///
/// Each worker keeps its own priority queue sorted by *bottom level* -- the
/// longest weighted path from a task to any sink.  A newly ready task is
/// placed on the worker that produced its last dependency, and each worker
/// always runs its own most urgent task.  Idle workers steal the most urgent
/// task from a peer.
///
/// Running the heaviest-remaining task first is the Graham list-scheduling
/// heuristic; on irregular graphs with uneven task costs it shortens the
/// makespan relative to FIFO/work-stealing, at the price of ignoring data
/// locality (which the locality-aware policy restores).
class CriticalPathPolicy final : public SchedulingPolicy {
public:
    void on_dag_ready(const DAG&, std::size_t workers) override { q_.init(workers); }

    void on_ready(const TaskHandle& task, WorkerId producer) override {
        q_.insert(producer % q_.workers(), task);
    }

    TaskHandle try_acquire(WorkerId worker) override {
        TaskHandle t = q_.take_own(worker);
        return t ? t : q_.steal(worker);
    }

    std::size_t size() const noexcept override { return q_.size(); }
    std::size_t steals() const noexcept override { return q_.steals(); }
    const char* name() const noexcept override { return "critical-path"; }

private:
    detail::PerWorkerQueues q_;
};

} // namespace

std::shared_ptr<SchedulingPolicy> make_critical_path_policy() {
    return std::make_shared<CriticalPathPolicy>();
}

} // namespace pulse
