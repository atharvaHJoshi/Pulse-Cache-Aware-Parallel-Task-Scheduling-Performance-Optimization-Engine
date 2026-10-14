#pragma once

#include "pulse/task.hpp"

#include <cstddef>
#include <string>

namespace pulse {

class DAG;
class Runtime;

/// A scheduling policy decides *which* ready task a worker should run next.
///
/// The runtime owns threads, dependency resolution and instrumentation; a
/// policy only owns the ordering/dispatch decision.  This separation lets the
/// four strategies in this project share one correct execution engine while
/// differing purely in scheduling behaviour.
///
/// Thread-safety contract:
///   * on_ready() is invoked by the runtime while holding the runtime wake
///     mutex, so implementations may assume no concurrent on_ready() calls.
///   * try_acquire() is called by every worker without the wake mutex, so it
///     must be internally synchronised.
///   * on_start()/on_finish() run on the executing worker thread.
class SchedulingPolicy {
public:
    virtual ~SchedulingPolicy() = default;

    /// Called once before the run starts so locality/priority policies can
    /// precompute per-task metadata.
    virtual void on_dag_ready(const DAG& /*dag*/, std::size_t /*workers*/) {}

    /// A task's dependencies are satisfied.  `producer` is the worker that
    /// completed the last dependency (or 0 for source tasks) and can be used as
    /// a locality hint.
    virtual void on_ready(const TaskHandle& task, WorkerId producer) = 0;

    /// Return the next task for `worker`, or nullptr if nothing is available.
    /// Must not block.
    virtual TaskHandle try_acquire(WorkerId worker) = 0;

    /// Number of tasks currently queued in the policy (best-effort).
    virtual std::size_t size() const noexcept = 0;

    virtual void on_start(WorkerId /*worker*/, const TaskHandle& /*task*/) {}
    virtual void on_finish(WorkerId /*worker*/, const TaskHandle& /*task*/) {}

    virtual std::size_t steals() const noexcept { return 0; }
    virtual std::size_t migrations() const noexcept { return 0; }
    virtual const char* name() const noexcept = 0;
};

} // namespace pulse
