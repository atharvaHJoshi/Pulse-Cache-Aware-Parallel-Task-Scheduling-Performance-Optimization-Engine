#pragma once

#include "pulse/dag.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace pulse {

/// Shared read-mostly memory pool used to create realistic cache traffic.
///
/// Regions are large flat arrays.  Workload tasks read contiguous slices of a
/// region; when two tasks that touch the same region run on different cores
/// they contend for the same cache lines, which is exactly the effect the
/// locality-aware scheduler tries to avoid.
class MemoryPool {
public:
    MemoryPool(std::size_t regions, std::size_t region_bytes);

    std::size_t region_count() const noexcept { return regions_.size(); }
    double* region(DataId id) noexcept { return regions_[id].data(); }
    std::size_t region_elems() const noexcept { return region_elems_; }

    /// Sum a contiguous slice of a region; returns a value the caller must
    /// consume so the compiler cannot elide the memory traffic.
    double touch(DataId id, std::size_t slice, std::size_t num_slices) const;

    /// Repeatedly update a single element, `iters` times.
    ///
    /// Distinct tasks are given distinct slots, so this is free of data races,
    /// but eight consecutive doubles share one 64-byte cache line.  A scheduler
    /// that spreads same-region tasks across cores therefore pays heavy
    /// false-sharing traffic, while one that co-locates them does not.
    double mutate(DataId id, std::size_t slot, std::size_t iters, double delta);

private:
    std::size_t region_elems_;
    std::vector<std::vector<double>> regions_;
};

struct WorkloadTask {
    std::uint64_t cost_ns;
    std::function<void()> body;
};

/// A named, replayable workload factory.  Each call returns a fresh DAG so a
/// benchmark can run the same shape against every scheduler.
struct Workload {
    std::string name;
    std::string description;
    std::function<DAG(std::uint64_t seed, const std::shared_ptr<MemoryPool>& pool)> build;
};

/// Registry of standard workloads used by the benchmark driver.
std::vector<Workload> standard_workloads();

// Individual generators ------------------------------------------------------

/// Strictly sequential chain: exposes scheduling overhead and critical path.
DAG build_chain(std::size_t n, std::uint64_t cost_ns);

/// Complete binary fork/join tree of the given depth: balanced, plenty of
/// parallel slack.
DAG build_fork_join(std::size_t depth, std::uint64_t cost_ns);

/// Layered DAG: `levels` generations of `width` tasks with random edges only
/// between adjacent generations.
DAG build_layered(std::size_t levels, std::size_t width, std::uint64_t cost_ns,
                  std::uint64_t seed);

/// Random DAG with `n` tasks and approximately `avg_out` successors per task,
/// respecting a topological ordering.
DAG build_random(std::size_t n, std::size_t avg_out, std::uint64_t cost_ns,
                 std::uint64_t seed);

/// Locality-sensitive dataflow graph: tasks read slices of shared regions and
/// depend on earlier tasks that share a region, so good schedulers can keep
/// them cached.
DAG build_dataflow(std::size_t n, std::size_t regions, const std::shared_ptr<MemoryPool>& pool,
                   std::uint64_t seed);

/// Unbalanced graph: a long chain of expensive tasks feeding a sink, plus many
/// cheap independent tasks.  A scheduler that runs cheap work first (arrival
/// order) delays the heavy chain; one that respects the critical path starts it
/// immediately.  This is where bottom-level priority pays off.
DAG build_straggler(std::size_t chain_len, std::uint64_t chain_cost, std::size_t light_count,
                    std::uint64_t light_cost);

/// Contended, dependency-free workload: every task in a region reads and
/// mutates that region, but tasks are otherwise independent.  Producer
/// locality is useless here (all tasks are sources), so only a scheduler that
/// co-locates same-region tasks on one core avoids cross-core cache-line
/// ping-pong.
DAG build_contended(std::size_t regions, std::size_t tasks_per_region,
                    const std::shared_ptr<MemoryPool>& pool);

} // namespace pulse
