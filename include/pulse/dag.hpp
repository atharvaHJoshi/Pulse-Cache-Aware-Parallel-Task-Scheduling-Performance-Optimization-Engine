#pragma once

#include "pulse/task.hpp"

#include <memory>
#include <unordered_map>
#include <vector>

namespace pulse {

/// Directed acyclic graph of tasks.
///
/// The graph owns every task and owns the reverse (successor) adjacency used by
/// the runtime to wake dependents when a task completes.  Call finalize() once
/// after construction: it validates the graph, builds reverse edges and
/// computes critical-path lengths bottom-up.
class DAG {
public:
    DAG() = default;

    /// Create a task and return a handle to it.
    TaskHandle create_task(std::function<void()> func, std::uint64_t cost_ns = 1000);

    /// Declare that `to` may only start after `from` has completed.
    void add_dependency(TaskId from, TaskId to);

    /// Declare that a task touches a data region (used for locality scoring).
    void add_data_access(TaskId tid, DataId did, std::size_t bytes = 64, bool write = false);

    TaskHandle get_task(TaskId id) const;

    const std::vector<TaskHandle>& tasks() const noexcept { return tasks_; }
    const std::vector<std::vector<TaskId>>& successors() const noexcept { return successors_; }
    const std::vector<std::vector<TaskId>>& predecessors() const noexcept { return predecessors_; }

    std::size_t size() const noexcept { return tasks_.size(); }
    std::size_t edge_count() const noexcept { return edge_count_; }

    /// Length of the longest path (in estimated cost) to any sink.
    std::uint64_t critical_path() const noexcept { return total_critical_path_; }

    /// Build adjacency, detect cycles and compute critical paths.  Throws
    /// std::invalid_argument on a cyclic graph or an out-of-range dependency.
    void finalize();

    /// Reset per-run execution state (dependency counters and task states) so
    /// the same finalized graph can be executed again.  Does not recompute
    /// critical paths -- this is what makes deterministic replay cheap.
    void reset_state();

private:
    TaskId next_id_{0};
    std::size_t edge_count_{0};
    std::uint64_t total_critical_path_{0};
    std::vector<TaskHandle> tasks_;
    std::vector<std::vector<TaskId>> successors_;
    std::vector<std::vector<TaskId>> predecessors_;
    std::unordered_map<TaskId, TaskHandle> task_map_;
};

} // namespace pulse
