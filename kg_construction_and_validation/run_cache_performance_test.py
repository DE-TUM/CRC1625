"""
Performance test for the workflow-instance validation cache.

It answers the question the Web UI raises: when the workflows page lists every instance of a model and
shows its "Validation status" (see handover_workflows_validation_webui/main_page.py), how much does the
validation cache help as the number of instances per model grows?

For a single workflow model with a varying number of instances, it measures the cost of retrieving the
validation status of *all* of them at once (exactly what `populate_workflow_instances_table` does via
`asyncio.gather` over `is_workflow_instance_valid`), in two regimes:

  - WITHOUT cache: every instance is recomputed (full SHACL validation in a process pool)
  - WITH cache:    every instance is a cache hit (we prime the cache first, so no validation runs)

Two kinds of metric are recorded, per number of instances:

  1. Speed (latency)  - wall-clock time of the gather, i.e. what the user waits for
  2. Compute          - total CPU-seconds (this process + its validation worker processes) and peak RSS,
                        i.e. the server load the cache avoids. The process pool that the no-cache path
                        spawns is invisible to wall-clock once work is parallelized, but its CPU and
                        memory cost still scale with the number of instances.

Two sweeps are run, each writing its own results .json and three PDF plots (latency, CPU-seconds and peak
RSS), with one line per regime and the swept dimension on the x-axis:
  - by_instances: a varying number of instances per model, at a fixed step count (scaling with breadth)
  - by_steps:     a single instance, at a varying number of steps                 (scaling with depth)

Reuses the scenario generators from `run_handover_workflows_validation_test.py` (as `run_cache_test.py`
does) and the cache plumbing from `workflows_validation`. The measured operation mirrors the Web UI; the
instance-loading query is deliberately left out of the timed window since it is identical for both
regimes and would only add a constant offset.

Assumes the RDF datastore (e.g. Virtuoso) is already running. *All* datastore contents will be cleared.

Run as a CLI application, e.g.:
    python run_cache_performance_test.py --output_dir ./cache_performance_test --repetitions 3
"""
import argparse
import asyncio
import json
import logging
import os
import resource
import sys
import threading
import time
from statistics import fmean, pstdev

import psutil

import matplotlib
matplotlib.use("Agg")  # Headless: render straight to file, never open a window
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, NullFormatter

from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel
from workflows_validation.workflows_validator import is_workflow_instance_valid

from cache_performance_test.cache_test_common import (
    build_model_with_instances,
    reload_instances,
    store_in_cache,
    mark_model_instances_stale,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

DEFAULT_N_STEPS = 5
DEFAULT_INSTANCE_COUNTS = [1, 5, 10, 25, 50, 100]
DEFAULT_STEP_COUNTS = [1, 5, 10, 25, 50, 100]
DEFAULT_REPETITIONS = 3
DEFAULT_OUTPUT_DIR = os.path.join(module_dir, "./cache_performance_test/results")


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #
def _cpu_seconds() -> float:
    """
    Total CPU seconds consumed by this process and its (reaped) children. The validation process pool
    reaps its workers on shutdown within `is_workflow_instance_valid`, so their CPU is included here.
    Excludes the separate RDF-store server process, which is not a child of this one.
    """
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


class _RssSampler(threading.Thread):
    """
    Background sampler of the resident memory of this process plus its (short-lived) validation worker
    processes, used to capture the peak RSS during a validation pass. Sampling beats `ru_maxrss`, which
    is a non-resettable high-water mark and would leak the no-cache peak into the cache run.
    """
    def __init__(self, interval: float = 0.05):
        super().__init__(daemon=True)
        self.interval = interval
        self._stop = threading.Event()
        self.peak_rss = 0
        self._proc = psutil.Process(os.getpid())

    def _tree_rss(self) -> int:
        total = 0
        try:
            total += self._proc.memory_info().rss
            for child in self._proc.children(recursive=True):
                try:
                    total += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except psutil.NoSuchProcess:
            pass
        return total

    def run(self):
        while not self._stop.is_set():
            self.peak_rss = max(self.peak_rss, self._tree_rss())
            self._stop.wait(self.interval)

    def stop(self) -> int:
        self._stop.set()
        self.join()
        return self.peak_rss


async def measure_validation_pass(model: WorkflowModel, instances: list[WorkflowInstance]) -> dict:
    """
    Validates the status of all instances at once (mirroring the Web UI's `asyncio.gather`) and records
    the latency, CPU-seconds and peak RSS of that pass.
    """
    sampler = _RssSampler()
    sampler.start()
    cpu_before = _cpu_seconds()
    start = time.perf_counter()

    await asyncio.gather(*(
        is_workflow_instance_valid(model, instance, return_individual_results=False)
        for instance in instances
    ))

    latency = time.perf_counter() - start
    cpu_seconds = _cpu_seconds() - cpu_before
    peak_rss = sampler.stop()

    return {"latency_s": latency, "cpu_s": cpu_seconds, "peak_rss_bytes": peak_rss}


# --------------------------------------------------------------------------- #
# Experiment
# --------------------------------------------------------------------------- #
async def _build_model(n_instances: int, n_steps: int) -> WorkflowModel:
    """Builds one model with `n_instances` instances of `n_steps` steps in the store, returning the model."""
    model, _, _ = await build_model_with_instances(n_instances, n_steps)
    return model


async def run_experiment(x_values: list[int], build_model_for_x, repetitions: int,
                         x_name: str = "x") -> list[dict]:
    """
    Generic cache benchmark sweep. For each value `x` in `x_values`, `build_model_for_x(x)` sets up a model
    (with its data and instances) in the store and returns it; then `repetitions` no-cache passes and
    `repetitions` cache (all-hits) passes are measured over it, and the raw per-pass measurements are
    returned, each row tagged with the swept value under key "x".

    The swept dimension is whatever `build_model_for_x` varies (number of instances, or number of steps of a
    single instance). `x_name` is only used in log messages.
    """
    results: list[dict] = []

    for x in x_values:
        logging.info("Setting up experiment point %s=%s ...", x_name, x)
        model = await build_model_for_x(x)

        # WITHOUT cache: validation now stores its result on a miss, so mark every instance stale before
        # each pass to keep it a cold run
        no_cache_runs = []
        for rep in range(repetitions):
            await mark_model_instances_stale(model)
            instances = list((await reload_instances(model)).values())
            measurement = await measure_validation_pass(model, instances)
            no_cache_runs.append(measurement)
            logging.info("[no-cache] %s=%s rep=%d latency=%.3fs cpu=%.3fs peak_rss=%.1fMiB",
                         x_name, x, rep, measurement["latency_s"], measurement["cpu_s"],
                         measurement["peak_rss_bytes"] / 1024 ** 2)

        # WITH cache: prime every instance once, then reload (cache fields present) before each all-hits pass
        await store_in_cache(list((await reload_instances(model)).values()))
        cache_runs = []
        for rep in range(repetitions):
            instances = list((await reload_instances(model)).values())
            measurement = await measure_validation_pass(model, instances)
            cache_runs.append(measurement)
            logging.info("[cache]    %s=%s rep=%d latency=%.3fs cpu=%.3fs peak_rss=%.1fMiB",
                         x_name, x, rep, measurement["latency_s"], measurement["cpu_s"],
                         measurement["peak_rss_bytes"] / 1024 ** 2)

        results.append({"x": x, "no_cache": no_cache_runs, "cache": cache_runs})

    return results


# --------------------------------------------------------------------------- #
# Aggregation, saving and plotting
# --------------------------------------------------------------------------- #
def _mean_std(values: list[float]) -> tuple[float, float]:
    """Mean and (population) standard deviation, tolerant of a single repetition."""
    return fmean(values), (pstdev(values) if len(values) > 1 else 0.0)


def aggregate(results: list[dict]) -> dict:
    """Reduces the raw per-pass measurements to mean/std series ready for plotting."""
    aggregated = {"x": [r["x"] for r in results]}

    for regime in ("no_cache", "cache"):
        for metric in ("latency_s", "cpu_s", "peak_rss_bytes"):
            means, stds = [], []
            for r in results:
                mean, std = _mean_std([run[metric] for run in r[regime]])
                means.append(mean)
                stds.append(std)
            aggregated[f"{regime}_{metric}_mean"] = means
            aggregated[f"{regime}_{metric}_std"] = stds

    return aggregated


def save_latency_plot(aggregated: dict, path: str, x_label: str) -> None:
    """Two lines (no-cache vs cache) of validation-status latency against the swept dimension."""
    ns = aggregated["x"]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(ns, aggregated["no_cache_latency_s_mean"], yerr=aggregated["no_cache_latency_s_std"],
                marker='o', capsize=3, label="Without cache (recompute)")
    ax.errorbar(ns, aggregated["cache_latency_s_mean"], yerr=aggregated["cache_latency_s_std"],
                marker='s', capsize=3, label="With cache (all hits)")
    ax.set_xlabel(x_label)
    ax.set_yscale("log")
    # Plain numbers on the log y-axis (1, 10, 100, ...), and suppress minor-tick labels so narrow-range
    # plots (e.g. RSS spans ~1 decade) show clean decade labels instead of 2x10^3, 3x10^2, etc.
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylabel("Validation-status latency (s)")
    ax.set_title("Validation-status retrieval latency: cache vs no cache")
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    logging.info("Latency plot saved to: %s", path)


def save_cpu_plot(aggregated: dict, path: str, x_label: str) -> None:
    """Two lines (no-cache vs cache) of total CPU-seconds against the swept dimension."""
    ns = aggregated["x"]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(ns, aggregated["no_cache_cpu_s_mean"], yerr=aggregated["no_cache_cpu_s_std"],
                marker='o', capsize=3, label="Without cache (recompute)")
    ax.errorbar(ns, aggregated["cache_cpu_s_mean"], yerr=aggregated["cache_cpu_s_std"],
                marker='s', capsize=3, label="With cache (all hits)")
    ax.set_xlabel(x_label)
    ax.set_yscale("log")
    # Plain numbers on the log y-axis (1, 10, 100, ...), and suppress minor-tick labels so narrow-range
    # plots (e.g. RSS spans ~1 decade) show clean decade labels instead of 2x10^3, 3x10^2, etc.
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylabel("Total CPU time (s)")
    ax.set_title("Validation-status compute cost (CPU-seconds): cache vs no cache")
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    logging.info("CPU-time plot saved to: %s", path)


def save_rss_plot(aggregated: dict, path: str, x_label: str) -> None:
    """Two lines (no-cache vs cache) of peak resident memory against the swept dimension."""
    ns = aggregated["x"]

    no_cache_rss_mib = [v / 1024 ** 2 for v in aggregated["no_cache_peak_rss_bytes_mean"]]
    cache_rss_mib = [v / 1024 ** 2 for v in aggregated["cache_peak_rss_bytes_mean"]]
    no_cache_rss_std_mib = [v / 1024 ** 2 for v in aggregated["no_cache_peak_rss_bytes_std"]]
    cache_rss_std_mib = [v / 1024 ** 2 for v in aggregated["cache_peak_rss_bytes_std"]]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(ns, no_cache_rss_mib, yerr=no_cache_rss_std_mib,
                marker='o', capsize=3, label="Without cache (recompute)")
    ax.errorbar(ns, cache_rss_mib, yerr=cache_rss_std_mib,
                marker='s', capsize=3, label="With cache (all hits)")
    ax.set_xlabel(x_label)
    ax.set_yscale("log")
    # Plain numbers on the log y-axis (1, 10, 100, ...), and suppress minor-tick labels so narrow-range
    # plots (e.g. RSS spans ~1 decade) show clean decade labels instead of 2x10^3, 3x10^2, etc.
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylabel("Peak resident memory (MiB)")
    ax.set_title("Validation-status compute cost (peak RSS): cache vs no cache")
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    logging.info("Peak-RSS plot saved to: %s", path)


def save_experiment(raw_results: list[dict], output_dir: str, name_prefix: str, x_label: str,
                    config: dict, skip_compute_plot: bool) -> None:
    """
    Aggregates one experiment's raw results, writes its results .json and its (one or three) plots. All
    outputs are suffixed with `name_prefix` (e.g. "by_instances" / "by_steps"), and `x_label` is the text
    written on the x-axis of each plot.
    """
    aggregated = aggregate(raw_results)

    results_path = os.path.join(output_dir, f"cache_performance_results_{name_prefix}.json")
    with open(results_path, "w") as f:
        json.dump({"config": config, "x_label": x_label, "raw_results": raw_results, "aggregated": aggregated},
                  f, indent=4)
    logging.info("Results saved to: %s", results_path)

    save_latency_plot(aggregated, os.path.join(output_dir, f"cache_latency_{name_prefix}.pdf"), x_label)
    if not skip_compute_plot:
        save_cpu_plot(aggregated, os.path.join(output_dir, f"cache_cpu_{name_prefix}.pdf"), x_label)
        save_rss_plot(aggregated, os.path.join(output_dir, f"cache_rss_{name_prefix}.pdf"), x_label)


async def main(args) -> None:
    instance_counts = [int(n) for n in args.instance_counts.split(",") if n.strip()]
    step_counts = [int(n) for n in args.step_counts.split(",") if n.strip()]
    os.makedirs(args.output_dir, exist_ok=True)

    # Experiment A: vary the number of instances per model, at a fixed step count (scaling with breadth)
    if args.experiments in ("both", "instances"):
        logging.info("Experiment 'by_instances': instance_counts=%s, n_steps=%d, repetitions=%d",
                     instance_counts, args.n_steps, args.repetitions)
        raw_results = await run_experiment(instance_counts,
                                           lambda n: _build_model(n, args.n_steps),
                                           args.repetitions, x_name="instances")
        save_experiment(raw_results, args.output_dir, "by_instances",
                        "Number of instances per workflow model",
                        {"instance_counts": instance_counts, "n_steps": args.n_steps,
                         "repetitions": args.repetitions},
                        args.skip_compute_plot)

    # Experiment B: a single instance, vary the number of steps (scaling with workflow depth)
    if args.experiments in ("both", "steps"):
        logging.info("Experiment 'by_steps' (single instance): step_counts=%s, repetitions=%d",
                     step_counts, args.repetitions)
        raw_results = await run_experiment(step_counts,
                                           lambda s: _build_model(1, s),
                                           args.repetitions, x_name="steps")
        save_experiment(raw_results, args.output_dir, "by_steps",
                        "Number of steps per workflow model (single instance)",
                        {"step_counts": step_counts, "n_instances": 1,
                         "repetitions": args.repetitions},
                        args.skip_compute_plot)

    logging.info("Cache performance test complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Speed and compute performance test for the validation cache.")
    parser.add_argument(
        "--output_dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the results .json files and the plots are written"
    )
    parser.add_argument(
        "--instance_counts",
        default=",".join(str(n) for n in DEFAULT_INSTANCE_COUNTS),
        help="Comma-separated instance counts for the by_instances experiment (x-axis)"
    )
    parser.add_argument(
        "--step_counts",
        default=",".join(str(n) for n in DEFAULT_STEP_COUNTS),
        help="Comma-separated step counts for the single-instance by_steps experiment (x-axis)"
    )
    parser.add_argument(
        "--n_steps",
        type=int,
        default=DEFAULT_N_STEPS,
        help="Fixed number of steps per model in the by_instances experiment"
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=DEFAULT_REPETITIONS,
        help="How many times each pass is repeated, for averaging"
    )
    parser.add_argument(
        "--experiments",
        choices=["both", "instances", "steps"],
        default="both",
        help="Which experiment(s) to run"
    )
    parser.add_argument(
        "--skip_compute_plot",
        action="store_true",
        default=False,
        help="Only render the latency plot (compute metrics are still recorded in the .json)"
    )
    args = parser.parse_args()

    asyncio.run(main(args))

    logging.info("Cache performance test complete.")
