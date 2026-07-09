"""
Performance test for the workflow-instance validation cache.

It answers the question the Web UI raises: when the workflows page lists every instance of a model and
shows its "Validation status" (see handover_workflows_validation_webui/main_page.py), how much does the
validation cache help as the number of instances per model grows?

For a single workflow model with a varying number of instances, it measures the cost of retrieving the
validation status of *all* of them at once (exactly what `populate_workflow_instances_table` does via
`asyncio.gather` over `is_workflow_instance_valid`), at one or more cache-hit ratios:

A hit ratio of p% means p% of the instances hold a valid cached result (hits, no validation runs) while
the rest are recomputed (misses: full SHACL validation in a process pool, including writing the fresh
result back to the cache). 0% is the all-miss ("without cache") regime, 100% the all-hit regime, and
anything in between models a partially warm cache, as after an edit or a partial invalidation.

`--n_users` additionally simulates that many users loading the page at the same time: each pass validates
that many independently loaded copies of every instance concurrently, all on one event loop, exactly like
simultaneous users of the single-process web UI.

Two kinds of metric are recorded, per number of instances:

  1. Speed (latency)  - wall-clock time of the gather, i.e. what the user waits for
  2. Compute          - total CPU-seconds (this process + its validation worker processes) and peak RSS,
                        i.e. the server load the cache avoids. The process pool that the no-cache path
                        spawns is invisible to wall-clock once work is parallelized, but its CPU and
                        memory cost still scale with the number of instances.

Two sweeps are run, each writing its own results .json and SVG plots (latency, CPU-seconds and peak RSS,
each rendered with a logarithmic and with a linear y-axis), with one line per cache-hit ratio and the
swept dimension on the x-axis. Filenames carry the experiment name, a "parallel" tag when --n_users > 1,
the y-axis scale and the run timestamp:
  - by_instances: a varying number of instances per model, at a fixed step count (scaling with breadth);
                  sweeps every ratio passed via --hit_ratios
  - by_steps:     a single instance, at a varying number of steps (scaling with depth); always runs only
                  0% and 100%, since one instance either hits or misses as a whole

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
from datetime import datetime
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
    mark_instances_stale,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

module_dir = os.path.dirname(__file__)

DEFAULT_N_STEPS = 5
DEFAULT_INSTANCE_COUNTS = [1, 2, 5, 10, 25, 50, 100]
DEFAULT_STEP_COUNTS = [1, 2, 5, 10, 25, 50, 75]
DEFAULT_HIT_RATIO_PERCENTS = [0, 50, 75, 90, 100]
DEFAULT_REPETITIONS = 3

METRICS = ("latency_s", "cpu_s", "peak_rss_bytes")
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


async def measure_validation_pass(model: WorkflowModel,
                                  user_instance_sets: list[list[WorkflowInstance]]) -> dict:
    """
    Validates the status of all instances at once for every simulated user, and records the latency,
    CPU-seconds and peak RSS of the whole pass.

    Each element of `user_instance_sets` is one simulated user: an independent page load doing the Web
    UI's `asyncio.gather` over its own copies of the instances. All users run concurrently on this one
    event loop, exactly like simultaneous users of the single-process web UI.
    """
    async def validate_all(instances: list[WorkflowInstance]):
        await asyncio.gather(*(
            is_workflow_instance_valid(model, instance, return_individual_results=False)
            for instance in instances
        ))

    sampler = _RssSampler()
    sampler.start()
    cpu_before = _cpu_seconds()
    start = time.perf_counter()

    await asyncio.gather(*(validate_all(instances) for instances in user_instance_sets))

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
                         hit_ratios: list[float], x_name: str = "x", n_users: int = 1) -> list[dict]:
    """
    Generic cache benchmark sweep. For each value `x` in `x_values`, `build_model_for_x(x)` sets up a model
    (with its data and instances) in the store and returns it; then, for every cache-hit ratio in
    `hit_ratios` (fractions in [0, 1]), `repetitions` passes over all instances are measured in which that
    share of the instances hit the cache and the rest are recomputed.

    The ratio is established by priming every instance's cache once and then, before every pass, marking a
    fixed subset of the instances stale: the first round(ratio * n) instances in IRI order hit, the rest
    miss. Re-staling before *every* pass is required because a measured miss re-caches itself (validation
    persists its result), which would otherwise turn the next pass into all hits. The cache-write cost of
    the misses is intentionally part of the measurement.

    `n_users` simulates that many independent users loading the page at the same time: each pass validates
    `n_users` freshly loaded copies of every instance concurrently on the shared event loop (like the
    web UI).

    Returns the raw per-pass measurements as one row per x value:
    {"x": x, "runs": {"<hit percent>": [measurement, ...]}}. The swept dimension is whatever
    `build_model_for_x` varies (number of instances, or number of steps of a single instance). `x_name` is
    only used in log messages.
    """
    results: list[dict] = []

    for x in x_values:
        logging.info("Setting up experiment point %s=%s ...", x_name, x)
        model = await build_model_for_x(x)

        # Prime every instance once so hits are available; misses are then created by staling a subset.
        # (After each measured pass all instances are cached again: hits were, misses re-cached themselves.)
        all_instances = list((await reload_instances(model)).values())
        await store_in_cache(all_instances)
        instances_in_iri_order = sorted(all_instances, key=lambda instance: str(instance.iri))

        runs_per_ratio: dict[str, list[dict]] = {}
        for ratio in hit_ratios:
            ratio_key = f"{100 * ratio:g}"
            n_hits = int(ratio * len(instances_in_iri_order) + 0.5)
            miss_instances = instances_in_iri_order[n_hits:]

            runs = []
            for rep in range(repetitions):
                await mark_instances_stale(miss_instances)
                # One freshly loaded, independent copy of the instances per simulated user, like every
                # browser tab loading the page for itself (kept outside the timed window)
                user_instance_sets = [list((await reload_instances(model)).values())
                                      for _ in range(n_users)]
                measurement = await measure_validation_pass(model, user_instance_sets)
                runs.append(measurement)
                logging.info("[hits=%s%% (%d/%d cached)] %s=%s users=%d rep=%d latency=%.3fs cpu=%.3fs peak_rss=%.1fMiB",
                             ratio_key, n_hits, len(instances_in_iri_order), x_name, x, n_users, rep,
                             measurement["latency_s"], measurement["cpu_s"],
                             measurement["peak_rss_bytes"] / 1024 ** 2)
            runs_per_ratio[ratio_key] = runs

        results.append({"x": x, "runs": runs_per_ratio})

    return results


# --------------------------------------------------------------------------- #
# Aggregation, saving and plotting
# --------------------------------------------------------------------------- #
def _mean_std(values: list[float]) -> tuple[float, float]:
    """Mean and (population) standard deviation, tolerant of a single repetition."""
    return fmean(values), (pstdev(values) if len(values) > 1 else 0.0)


def aggregate(results: list[dict]) -> dict:
    """
    Reduces the raw per-pass measurements to mean/std series ready for plotting:
    {"x": [...], "series": {"<hit percent>": {"<metric>_mean": [...], "<metric>_std": [...]}}}
    """
    aggregated = {"x": [r["x"] for r in results], "series": {}}

    ratio_keys = list(results[0]["runs"].keys()) if results else []
    for ratio_key in ratio_keys:
        series = {}
        for metric in METRICS:
            means, stds = [], []
            for r in results:
                mean, std = _mean_std([run[metric] for run in r["runs"][ratio_key]])
                means.append(mean)
                stds.append(std)
            series[f"{metric}_mean"] = means
            series[f"{metric}_std"] = stds
        aggregated["series"][ratio_key] = series

    return aggregated


def _hit_ratio_label(ratio_key: str) -> str:
    """Legend label for a hit-ratio series, spelling out the two extremes."""
    percent = float(ratio_key)
    if percent == 0:
        return "0% hits (recompute all)"
    if percent == 100:
        return "100% hits (all cached)"
    return f"{ratio_key}% hits"


def _save_series_plot(aggregated: dict, path: str, x_label: str,
                      metric: str, y_label: str, title: str, y_scale: float = 1.0,
                      title_suffix: str = "", y_log: bool = True) -> None:
    """
    One errorbar line per cache-hit ratio of `metric` against the swept dimension. The two reference
    regimes get dark anchor colors, intermediate ratios distinct medium-tone hues. `y_scale` divides the
    raw values (e.g. bytes -> MiB). `y_log` selects a logarithmic y-axis (making the near-zero cache
    lines visible) or a plain linear one.
    """
    xs = aggregated["x"]
    ratio_keys = sorted(aggregated["series"].keys(), key=float)
    markers = ['o', 's', '^', 'D', 'v', 'P', 'X', '*']
    extreme_colors = {0.0: "#a50f15", 100.0: "#08519c"}  # dark red / dark blue
    intermediate_colors = ["#e08214", "#41ab5d", "#807dba", "#c51b8a", "#35978f", "#8c6d31"]

    fig, ax = plt.subplots(figsize=(8, 5))
    intermediate_index = 0
    for i, ratio_key in enumerate(ratio_keys):
        series = aggregated["series"][ratio_key]
        percent = float(ratio_key)
        if percent in extreme_colors:
            color = extreme_colors[percent]
        else:
            color = intermediate_colors[intermediate_index % len(intermediate_colors)]
            intermediate_index += 1
        ax.errorbar(xs,
                    [v / y_scale for v in series[f"{metric}_mean"]],
                    yerr=[v / y_scale for v in series[f"{metric}_std"]],
                    marker=markers[i % len(markers)], capsize=3, color=color,
                    label=_hit_ratio_label(ratio_key))

    ax.set_xlabel(x_label)
    if y_log:
        ax.set_yscale("log")
        # Plain numbers on the log y-axis (1, 10, 100, ...), and suppress minor-tick labels so narrow-range
        # plots (e.g. RSS spans ~1 decade) show clean decade labels instead of 2x10^3, 3x10^2, etc.
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylabel(y_label)
    ax.set_title(title + title_suffix)
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    logging.info("Plot saved to: %s", path)


def save_latency_plot(aggregated: dict, path: str, x_label: str, title_suffix: str = "",
                      y_log: bool = True) -> None:
    """Validation-status latency against the swept dimension, one line per cache-hit ratio."""
    _save_series_plot(aggregated, path, x_label, "latency_s",
                      "Validation-status latency (s)",
                      "Latency",
                      title_suffix=title_suffix, y_log=y_log)


def save_cpu_plot(aggregated: dict, path: str, x_label: str, title_suffix: str = "",
                  y_log: bool = True) -> None:
    """Total CPU-seconds against the swept dimension, one line per cache-hit ratio."""
    _save_series_plot(aggregated, path, x_label, "cpu_s",
                      "Total CPU time (s)",
                      "CPU-Seconds",
                      title_suffix=title_suffix, y_log=y_log)


def save_rss_plot(aggregated: dict, path: str, x_label: str, title_suffix: str = "",
                  y_log: bool = True) -> None:
    """Peak resident memory against the swept dimension, one line per cache-hit ratio."""
    _save_series_plot(aggregated, path, x_label, "peak_rss_bytes",
                      "Peak resident memory (MiB)",
                      "Peak RSS",
                      y_scale=1024 ** 2, title_suffix=title_suffix, y_log=y_log)


def save_experiment(raw_results: list[dict], output_dir: str, name_prefix: str, x_label: str,
                    config: dict, skip_compute_plot: bool, n_users: int = 1,
                    run_timestamp: str = "") -> None:
    """
    Aggregates one experiment's raw results, writes its results .json and its plots. Every plot is
    rendered twice, once with a logarithmic and once with a linear y-axis. The filenames are built from
    `name_prefix` (e.g. "by_instances" / "by_steps"), a "parallel" tag when several concurrent users were
    simulated, the y-axis scale ("log" / "linear") and `run_timestamp`, so files of different runs and
    settings are distinguishable and never overwrite each other. `x_label` is the text written on the
    x-axis of each plot. With more than one simulated user, the plot titles say so as well.
    """
    aggregated = aggregate(raw_results)
    parallel_tag = "_parallel" if n_users > 1 else ""
    base_suffix = f"{name_prefix}{parallel_tag}"
    if run_timestamp:
        base_suffix += f"_{run_timestamp}"

    results_path = os.path.join(output_dir, f"cache_performance_results_{base_suffix}.json")
    with open(results_path, "w") as f:
        json.dump({"config": config, "x_label": x_label, "raw_results": raw_results, "aggregated": aggregated},
                  f, indent=4)
    logging.info("Results saved to: %s", results_path)

    title_suffix = f" ({n_users} concurrent users)" if n_users > 1 else ""

    def plot_suffix(scale: str) -> str:
        suffix = f"{name_prefix}{parallel_tag}_{scale}"
        return f"{suffix}_{run_timestamp}" if run_timestamp else suffix

    for scale, y_log in (("log", True), ("linear", False)):
        save_latency_plot(aggregated, os.path.join(output_dir, f"cache_latency_{plot_suffix(scale)}.svg"),
                          x_label, title_suffix, y_log=y_log)
        if not skip_compute_plot:
            save_cpu_plot(aggregated, os.path.join(output_dir, f"cache_cpu_{plot_suffix(scale)}.svg"),
                          x_label, title_suffix, y_log=y_log)
            save_rss_plot(aggregated, os.path.join(output_dir, f"cache_rss_{plot_suffix(scale)}.svg"),
                          x_label, title_suffix, y_log=y_log)


async def main(args) -> None:
    instance_counts = [int(n) for n in args.instance_counts.split(",") if n.strip()]
    step_counts = [int(n) for n in args.step_counts.split(",") if n.strip()]

    # Hit percentages -> deduplicated fractions in [0, 1], keeping the given order
    hit_ratio_percents: list[float] = []
    for token in args.hit_ratios.split(","):
        if not token.strip():
            continue
        percent = float(token)
        if not 0 <= percent <= 100:
            raise SystemExit(f"--hit_ratios values must be within 0..100, got: {token}")
        if percent not in hit_ratio_percents:
            hit_ratio_percents.append(percent)
    hit_ratios = [percent / 100 for percent in hit_ratio_percents]

    if args.n_users < 1:
        raise SystemExit(f"--n_users must be at least 1, got: {args.n_users}")

    os.makedirs(args.output_dir, exist_ok=True)

    # One timestamp per run, shared by both experiments, so outputs never overwrite earlier runs
    # and the files of one run group together
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Experiment A: vary the number of instances per model, at a fixed step count (scaling with breadth)
    if args.experiments in ("both", "instances"):
        logging.info("Experiment 'by_instances': instance_counts=%s, n_steps=%d, hit_ratios=%s%%, repetitions=%d, n_users=%d",
                     instance_counts, args.n_steps, hit_ratio_percents, args.repetitions, args.n_users)
        raw_results = await run_experiment(instance_counts,
                                           lambda n: _build_model(n, args.n_steps),
                                           args.repetitions, hit_ratios, x_name="instances",
                                           n_users=args.n_users)
        save_experiment(raw_results, args.output_dir, "by_instances",
                        "Number of instances per workflow model",
                        {"instance_counts": instance_counts, "n_steps": args.n_steps,
                         "hit_ratio_percents": hit_ratio_percents, "repetitions": args.repetitions,
                         "n_users": args.n_users},
                        args.skip_compute_plot, n_users=args.n_users, run_timestamp=run_timestamp)

    # Experiment B: a single instance, vary the number of steps (scaling with workflow depth).
    # A hit ratio is all-or-nothing for one instance, so this always runs exactly 0% and 100%.
    if args.experiments in ("both", "steps"):
        logging.info("Experiment 'by_steps' (single instance): step_counts=%s, repetitions=%d, n_users=%d",
                     step_counts, args.repetitions, args.n_users)
        raw_results = await run_experiment(step_counts,
                                           lambda s: _build_model(1, s),
                                           args.repetitions, [0.0, 1.0], x_name="steps",
                                           n_users=args.n_users)
        save_experiment(raw_results, args.output_dir, "by_steps",
                        "Number of steps per workflow model (single instance)",
                        {"step_counts": step_counts, "n_instances": 1,
                         "hit_ratio_percents": [0, 100], "repetitions": args.repetitions,
                         "n_users": args.n_users},
                        args.skip_compute_plot, n_users=args.n_users, run_timestamp=run_timestamp)

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
        "--hit_ratios",
        default=",".join(str(p) for p in DEFAULT_HIT_RATIO_PERCENTS),
        help="Comma-separated cache-hit percentages (0..100) for the by_instances experiment; the plots "
             "draw one line per percentage. The single-instance by_steps experiment always uses 0 and 100, "
             "since its one instance either hits or misses as a whole"
    )
    parser.add_argument(
        "--n_users",
        type=int,
        default=1,
        help="Number of simulated concurrent users. Each user validates its own freshly loaded copy of "
             "all instances at the same time on the shared event loop, mirroring several users opening "
             "the workflows page simultaneously (the web UI serves all users from one process and event "
             "loop). With misses, every user recomputes independently (no in-flight deduplication)"
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
