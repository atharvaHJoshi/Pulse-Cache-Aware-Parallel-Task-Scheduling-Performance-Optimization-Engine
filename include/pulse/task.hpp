#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace pulse {

using TaskId = std::size_t;
using DataId = std::size_t;
using WorkerId = std::size_t;

/// A contiguous region of data a task reads or writes.  The scheduler uses
/// these identifiers to keep tasks that share data on the same core, so the
/// bytes stay resident in that core's private cache instead of bouncing
/// between cores' coherence domains.
struct DataAccess {
    DataId id{0};
    std::size_t bytes{0};
    bool write{false};
};

enum class TaskState : std::uint8_t {
    Pending,    // waiting on dependencies
    Ready,      // dependencies satisfied, waiting for a worker
    Running,    // executing
    Completed,  // finished
    Failed      // handler threw
};

/// A single unit of work in the task graph.  Tasks are heap allocated once and
/// referenced through shared pointers; the atomics let the runtime resolve
/// dependencies without a global lock on the hot path.
struct Task {
    TaskId id{0};
    std::function<void()> func;
    std::uint64_t cost_ns{1000};            // static estimate, used by CP scoring
    std::uint64_t critical_path_len{0};     // bottom level: longest weighted path to a sink
    std::vector<TaskId> deps;               // tasks that must finish first
    std::vector<DataAccess> data_accesses;  // locality metadata
    std::size_t affinity_hint{0};           // preferred worker (0 == none)

    std::atomic<TaskState> state{TaskState::Pending};
    std::atomic<int> dep_count{0};

    // ---- instrumentation (written once, only after the task is scheduled) ----
    std::uint64_t ready_ns{0};
    std::uint64_t start_ns{0};
    std::uint64_t end_ns{0};
    WorkerId worker{0};
};

using TaskHandle = std::shared_ptr<Task>;

} // namespace pulse
