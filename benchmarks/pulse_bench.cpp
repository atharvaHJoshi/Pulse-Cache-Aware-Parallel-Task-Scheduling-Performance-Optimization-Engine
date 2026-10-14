#include "pulse/runtime.hpp"
#include "pulse/workload.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

struct Options {
    std::string workload_filter = "all";
    std::string policy_filter = "all";
    std::size_t workers = 0;      // 0 => hardware concurrency
    std::size_t repeats = 5;
    std::string out_csv = "data/pulse_results.csv";
    std::string dump_workload;   // when set, dump this DAG and exit
    std::uint64_t seed = 42;
};

std::shared_ptr<pulse::SchedulingPolicy> make_policy(const std::string& name) {
    if (name == "fifo") return pulse::make_fifo_policy();
    if (name == "work-stealing") return pulse::make_work_stealing_policy();
    if (name == "critical-path") return pulse::make_critical_path_policy();
    if (name == "locality-aware") return pulse::make_locality_aware_policy();
    return nullptr;
}

void print_usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " [options]\n"
        << "  --workload <name|all>   workload to run (default all)\n"
        << "  --policy <name|all>     scheduler to run (default all)\n"
        << "  --workers <n>           worker threads (default: hw concurrency)\n"
        << "  --repeats <n>           repetitions per cell (default 5)\n"
        << "  --seed <n>              RNG seed (default 42)\n"
        << "  --out <path>            CSV output path (default data/pulse_results.csv)\n"
        << "  --dump-dag <name>       print one DAG in text form (for the OCaml model)\n"
        << "  --list                  list workloads and policies\n";
}

void list_all() {
    std::cout << "Workloads:\n";
    for (const auto& w : pulse::standard_workloads()) {
        std::cout << "  " << w.name << "  -  " << w.description << "\n";
    }
    std::cout << "Policies:\n"
              << "  fifo\n  work-stealing\n  critical-path\n  locality-aware\n";
}

// Serialize a DAG in a simple line-oriented format understood by the OCaml
// reference model (ocaml/pulse_ref.ml).  Lines starting with '#' are comments.
void dump_dag(const pulse::DAG& dag, std::uint64_t seed) {
    std::printf("# pulse-dag v1 seed=%llu\n", static_cast<unsigned long long>(seed));
    std::printf("tasks %zu\n", dag.size());
    for (const auto& t : dag.tasks()) {
        std::printf("cost %zu %llu\n", t->id, static_cast<unsigned long long>(t->cost_ns));
    }
    for (const auto& t : dag.tasks()) {
        for (pulse::TaskId d : t->deps) {
            std::printf("edge %zu %zu\n", d, t->id);
        }
    }
    for (const auto& t : dag.tasks()) {
        for (const auto& a : t->data_accesses) {
            std::printf("region %zu %zu %d\n", t->id, a.id, a.write ? 1 : 0);
        }
    }
    std::printf("critical_path %llu\n",
                static_cast<unsigned long long>(dag.critical_path()));
    for (const auto& t : dag.tasks()) {
        std::printf("bottom %zu %llu\n", t->id,
                    static_cast<unsigned long long>(t->critical_path_len));
    }
}

} // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << name << "\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (a == "--workload") opt.workload_filter = next("--workload");
        else if (a == "--policy") opt.policy_filter = next("--policy");
        else if (a == "--workers") opt.workers = std::stoul(next("--workers"));
        else if (a == "--repeats") opt.repeats = std::stoul(next("--repeats"));
        else if (a == "--seed") opt.seed = std::stoull(next("--seed"));
        else if (a == "--out") opt.out_csv = next("--out");
        else if (a == "--dump-dag") opt.dump_workload = next("--dump-dag");
        else if (a == "--list") { list_all(); return 0; }
        else if (a == "-h" || a == "--help") { print_usage(argv[0]); return 0; }
        else { std::cerr << "unknown option: " << a << "\n"; print_usage(argv[0]); return 2; }
    }

    const std::size_t workers = opt.workers;
    std::vector<std::string> policies = {"fifo", "work-stealing", "critical-path",
                                         "locality-aware"};
    if (opt.policy_filter != "all") policies = {opt.policy_filter};

    auto workloads = pulse::standard_workloads();
    if (!opt.dump_workload.empty()) {
        std::shared_ptr<pulse::MemoryPool> pool =
            std::make_shared<pulse::MemoryPool>(64, 64 * 1024);
        auto it = std::find_if(workloads.begin(), workloads.end(),
                               [&](const pulse::Workload& w) {
                                   return w.name == opt.dump_workload;
                               });
        if (it == workloads.end()) {
            std::cerr << "unknown workload: " << opt.dump_workload << "\n";
            return 2;
        }
        pulse::DAG dag = it->build(opt.seed, pool);
        dump_dag(dag, opt.seed);
        return 0;
    }
    if (opt.workload_filter != "all") {
        workloads.erase(std::remove_if(workloads.begin(), workloads.end(),
                                       [&](const pulse::Workload& w) {
                                           return w.name != opt.workload_filter;
                                       }),
                        workloads.end());
    }
    if (workloads.empty()) {
        std::cerr << "no workloads selected\n";
        return 2;
    }

    std::shared_ptr<pulse::MemoryPool> pool = std::make_shared<pulse::MemoryPool>(64, 64 * 1024);

    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(opt.out_csv).parent_path(), ec);
    std::ofstream csv(opt.out_csv);
    csv << "workload,policy,workers,repeat,tasks,makespan_ns,total_work_ns,throughput,"
           "speedup,utilization,load_imbalance,p50_ns,p95_ns,p99_ns,p99_wait_ns,steals,"
           "migrations\n";

    std::cout << "workers=" << (workers == 0 ? std::thread::hardware_concurrency() : workers)
              << " repeats=" << opt.repeats << " seed=" << opt.seed << "\n";

    struct Agg {
        double makespan = 0, throughput = 0, utilization = 0, imbalance = 0;
        double idle_frac = 0;
        std::uint64_t p50 = 0, p95 = 0, p99 = 0, p99wait = 0;
        std::size_t steals = 0, migrations = 0;
        int n = 0;
    };

    for (const auto& w : workloads) {
        for (const auto& pname : policies) {
            Agg agg;
            for (std::size_t rep = 0; rep < opt.repeats; ++rep) {
                pulse::DAG dag = w.build(opt.seed + rep, pool);
                auto policy = make_policy(pname);
                pulse::Runtime rt(workers, policy);
                pulse::RunResult r = rt.run(dag);

                csv << w.name << ',' << pname << ','
                    << (workers == 0 ? std::thread::hardware_concurrency() : workers) << ',' << rep
                    << ',' << r.tasks << ',' << r.makespan_ns << ',' << r.total_work_ns << ','
                    << r.throughput() << ',' << r.speedup() << ',' << r.utilization() << ','
                    << r.load_imbalance() << ',' << r.p50_latency() << ',' << r.p95_latency() << ','
                    << r.p99_latency() << ',' << r.p99_wait() << ',' << r.steals << ','
                    << r.conflict_migrations << "\n";

                agg.makespan += static_cast<double>(r.makespan_ns);
                agg.throughput += r.throughput();
                agg.utilization += r.utilization();
                agg.imbalance += r.load_imbalance();
                agg.p50 += r.p50_latency();
                agg.p95 += r.p95_latency();
                agg.p99 += r.p99_latency();
                agg.p99wait += r.p99_wait();
                agg.steals += r.steals;
                agg.migrations += r.conflict_migrations;
                std::uint64_t idle = 0;
                for (auto v : r.worker_idle_ns) idle += v;
                double denom = static_cast<double>(r.makespan_ns) * r.workers;
                agg.idle_frac += denom > 0 ? static_cast<double>(idle) / denom : 0.0;
                agg.n++;
            }
            double n = std::max(1, agg.n);
            std::printf("%-16s %-15s  makespan=%9.3fms  tput=%9.0f/s  util=%.3f  idle=%.3f  "
                        "imb=%.3f  p50=%7lluns p95=%7lluns p99=%8lluns  steals=%zu migr=%zu\n",
                        w.name.c_str(), pname.c_str(), agg.makespan / n / 1e6,
                        agg.throughput / n, agg.utilization / n, agg.idle_frac / n,
                        agg.imbalance / n,
                        static_cast<unsigned long long>(agg.p50 / agg.n),
                        static_cast<unsigned long long>(agg.p95 / agg.n),
                        static_cast<unsigned long long>(agg.p99 / agg.n), agg.steals / agg.n,
                        agg.migrations / agg.n);
        }
    }

    csv.close();
    std::cout << "\nwrote " << opt.out_csv << "\n";
    return 0;
}
