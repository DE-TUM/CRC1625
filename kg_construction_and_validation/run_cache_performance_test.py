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

Results are saved to a .json file and rendered as two plots (latency, and CPU/peak-RSS), with the number
of instances per model on the x-axis and one line per regime.

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
import uuid
from statistics import fmean, pstdev

import psutil
from rdflib import Graph, URIRef

import matplotlib
matplotlib.use("Agg")  # Headless: render straight to file, never open a window
import matplotlib.pyplot as plt

from datastores.rdf import rdf_datastore_client
from datastores.rdf.rdf_datastore import WORKFLOWS_GRAPH_IRI, MAIN_GRAPH_IRI
from workflows_validation.common import dw_prefix
from workflows_validation.extra_functions import get_workflow_instances_assigned_to_model
from workflows_validation.validation_cache import compute_footprint_hash
from workflows_validation.workflow_instance import WorkflowInstance
from workflows_validation.workflow_model import WorkflowModel
from workflows_validation.workflows_validator import is_workflow_instance_valid, ValidationStatus

from run_handover_workflows_validation_test import (
    generate_handover_group_definition,
    generate_handover_group_triples,
    generate_workflow_model_and_instance_for_handover_group_definition,
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
DEFAULT_REPETITIONS = 3
DEFAULT_OUTPUT_DIR = os.path.join(module_dir, "./cache_performance_test")


# --------------------------------------------------------------------------- #
# Scenario construction (one model, N instances)
# --------------------------------------------------------------------------- #
def _make_entity_graph_unique(graph: Graph, entity_iri: URIRef, suffix: str) -> tuple[Graph, URIRef]:
    """
    `generate_handover_group_triples` reuses fixed IRIs (handover_workflow_instance, handover_group_<i>)
    for every call, so each instance would otherwise share the same entity data. This rewrites those
    colliding IRIs with a per-instance suffix, yielding a distinct entity graph (and its entity IRI) so
    every instance validates against its own data, like in reality.
    """
    base = str(dw_prefix)
    workflow_instance_iri = str(dw_prefix.handover_workflow_instance)
    handover_group_prefix = base + "handover_group_"

    def remap(node):
        if isinstance(node, URIRef):
            node_str = str(node)
            if node_str == workflow_instance_iri or node_str.startswith(handover_group_prefix):
                return URIRef(f"{node_str}__inst{suffix}")
        return node

    unique_graph = Graph()
    for s, p, o in graph:
        unique_graph.add((remap(s), remap(p), remap(o)))

    return unique_graph, remap(entity_iri)


async def setup_model_with_instances(n_instances: int, n_steps: int) -> tuple[WorkflowModel, list[WorkflowInstance]]:
    """
    Clears both graphs and builds a single workflow model with `n_instances` instances assigned to it,
    each owning its own (valid) handover-group data. Loads the data, the model and the instances into the
    store. The instances have no cache yet, so their first validation is a miss.
    """
    await rdf_datastore_client.clear_triples()
    await rdf_datastore_client.clear_triples(WORKFLOWS_GRAPH_IRI)

    # One shared model. The generator is deterministic for a given definition (fixed step IRIs), so we
    # build the model once and reuse it across all instances.
    definition = generate_handover_group_definition(n_steps)
    model, _ = generate_workflow_model_and_instance_for_handover_group_definition(definition, dw_prefix["__model_seed_entity"])

    instances: list[WorkflowInstance] = []
    for i in range(n_instances):
        main_graph, entity_iri = generate_handover_group_triples(definition)
        main_graph, entity_iri = _make_entity_graph_unique(main_graph, entity_iri, str(i))

        # Reuse the generator to build the instance + its step assignments, then point it at the shared
        # model. The model it returns is identical to `model` (same fixed step IRIs) and discarded.
        _, instance = generate_workflow_model_and_instance_for_handover_group_definition(definition, entity_iri)
        instance.workflow_model_iri = model.iri

        ttl_path = f"{uuid.uuid4().hex}.ttl"
        main_graph.serialize(destination=ttl_path, format='turtle')
        await rdf_datastore_client.upload_file(ttl_path, graph_iri=MAIN_GRAPH_IRI, delete_file_after_upload=True)

        instances.append(instance)

    await rdf_datastore_client.launch_update(model.get_insert_query())
    for instance in instances:
        await rdf_datastore_client.launch_update(instance.get_insert_query())

    return model, instances


async def reload_instances(model: WorkflowModel) -> list[WorkflowInstance]:
    """
    Re-reads the model's instances from the store, so their cache fields match what the UI sees on
    reload. Done before every timed pass and kept out of the timed window.
    """
    instances = await get_workflow_instances_assigned_to_model(model, rdf_datastore_client.launch_query)
    return list(instances.values())


async def prime_cache(instances: list[WorkflowInstance]) -> None:
    """
    Does what the UI does after validating: stores a fresh (non-stale) cache result for every instance,
    so the next validation pass is an all-hits run. The instances are valid by construction, so the
    stored status is `Valid`.
    """
    for instance in instances:
        footprint_hash = await compute_footprint_hash(instance.iri)
        instance.mark_validated(ValidationStatus.Valid.name, footprint_hash)
        await rdf_datastore_client.launch_update(instance.get_cache_update_query())


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
async def run_experiment(instance_counts: list[int], n_steps: int, repetitions: int) -> list[dict]:
    """
    For each number of instances, measures `repetitions` no-cache passes and `repetitions` cache (all
    hits) passes over the same data, and returns the raw per-pass measurements.
    """
    results: list[dict] = []

    for n_instances in instance_counts:
        logging.info("Setting up model with %d instance(s) of %d steps...", n_instances, n_steps)
        model, _ = await setup_model_with_instances(n_instances, n_steps)

        # WITHOUT cache: reload fresh (uncached) instances before every pass, so each is a true cold run
        no_cache_runs = []
        for rep in range(repetitions):
            instances = await reload_instances(model)
            measurement = await measure_validation_pass(model, instances)
            no_cache_runs.append(measurement)
            logging.info("[no-cache] n=%d rep=%d latency=%.3fs cpu=%.3fs peak_rss=%.1fMiB",
                         n_instances, rep, measurement["latency_s"], measurement["cpu_s"],
                         measurement["peak_rss_bytes"] / 1024 ** 2)

        # WITH cache: prime once, then reload (cache fields present) before every all-hits pass
        await prime_cache(await reload_instances(model))
        cache_runs = []
        for rep in range(repetitions):
            instances = await reload_instances(model)
            measurement = await measure_validation_pass(model, instances)
            cache_runs.append(measurement)
            logging.info("[cache]    n=%d rep=%d latency=%.3fs cpu=%.3fs peak_rss=%.1fMiB",
                         n_instances, rep, measurement["latency_s"], measurement["cpu_s"],
                         measurement["peak_rss_bytes"] / 1024 ** 2)

        results.append({"n_instances": n_instances, "no_cache": no_cache_runs, "cache": cache_runs})

    return results


# --------------------------------------------------------------------------- #
# Aggregation, saving and plotting
# --------------------------------------------------------------------------- #
def _mean_std(values: list[float]) -> tuple[float, float]:
    """Mean and (population) standard deviation, tolerant of a single repetition."""
    return fmean(values), (pstdev(values) if len(values) > 1 else 0.0)


def aggregate(results: list[dict]) -> dict:
    """Reduces the raw per-pass measurements to mean/std series ready for plotting."""
    aggregated = {"n_instances": [r["n_instances"] for r in results]}

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


def save_latency_plot(aggregated: dict, path: str) -> None:
    """Two lines (no-cache vs cache) of validation-status latency against number of instances."""
    ns = aggregated["n_instances"]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(ns, aggregated["no_cache_latency_s_mean"], yerr=aggregated["no_cache_latency_s_std"],
                marker='o', capsize=3, label="Without cache (recompute)")
    ax.errorbar(ns, aggregated["cache_latency_s_mean"], yerr=aggregated["cache_latency_s_std"],
                marker='s', capsize=3, label="With cache (all hits)")
    ax.set_xlabel("Number of instances per workflow model")
    ax.set_ylabel("Validation-status latency (s)")
    ax.set_title("Validation-status retrieval latency: cache vs no cache")
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logging.info("Latency plot saved to: %s", path)


def save_compute_plot(aggregated: dict, path: str) -> None:
    """CPU-seconds and peak RSS (no-cache vs cache) against number of instances, side by side."""
    ns = aggregated["n_instances"]

    fig, (ax_cpu, ax_rss) = plt.subplots(1, 2, figsize=(13, 5))

    ax_cpu.errorbar(ns, aggregated["no_cache_cpu_s_mean"], yerr=aggregated["no_cache_cpu_s_std"],
                    marker='o', capsize=3, label="Without cache (recompute)")
    ax_cpu.errorbar(ns, aggregated["cache_cpu_s_mean"], yerr=aggregated["cache_cpu_s_std"],
                    marker='s', capsize=3, label="With cache (all hits)")
    ax_cpu.set_xlabel("Number of instances per workflow model")
    ax_cpu.set_ylabel("Total CPU time (s)")
    ax_cpu.set_title("Compute cost (CPU-seconds)")
    ax_cpu.grid(True, linestyle='--', alpha=0.4)
    ax_cpu.legend()

    no_cache_rss_mib = [v / 1024 ** 2 for v in aggregated["no_cache_peak_rss_bytes_mean"]]
    cache_rss_mib = [v / 1024 ** 2 for v in aggregated["cache_peak_rss_bytes_mean"]]
    no_cache_rss_std_mib = [v / 1024 ** 2 for v in aggregated["no_cache_peak_rss_bytes_std"]]
    cache_rss_std_mib = [v / 1024 ** 2 for v in aggregated["cache_peak_rss_bytes_std"]]

    ax_rss.errorbar(ns, no_cache_rss_mib, yerr=no_cache_rss_std_mib,
                    marker='o', capsize=3, label="Without cache (recompute)")
    ax_rss.errorbar(ns, cache_rss_mib, yerr=cache_rss_std_mib,
                    marker='s', capsize=3, label="With cache (all hits)")
    ax_rss.set_xlabel("Number of instances per workflow model")
    ax_rss.set_ylabel("Peak resident memory (MiB)")
    ax_rss.set_title("Compute cost (peak RSS)")
    ax_rss.grid(True, linestyle='--', alpha=0.4)
    ax_rss.legend()

    fig.suptitle("Validation-status compute cost: cache vs no cache")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    logging.info("Compute plot saved to: %s", path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Speed and compute performance test for the validation cache.")
    parser.add_argument(
        "--output_dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the results .json and the plots are written"
    )
    parser.add_argument(
        "--instance_counts",
        default=",".join(str(n) for n in DEFAULT_INSTANCE_COUNTS),
        help="Comma-separated numbers of instances per model to sweep (x-axis)"
    )
    parser.add_argument(
        "--n_steps",
        type=int,
        default=DEFAULT_N_STEPS,
        help="Number of steps of the (single) workflow model"
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=DEFAULT_REPETITIONS,
        help="How many times each pass is repeated, for averaging"
    )
    parser.add_argument(
        "--skip_compute_plot",
        action="store_true",
        default=False,
        help="Only render the latency plot (compute metrics are still recorded in the .json)"
    )
    args = parser.parse_args()

    instance_counts = [int(n) for n in args.instance_counts.split(",") if n.strip()]
    os.makedirs(args.output_dir, exist_ok=True)

    logging.info("Running cache performance test: instance_counts=%s, n_steps=%d, repetitions=%d",
                 instance_counts, args.n_steps, args.repetitions)

    raw_results = asyncio.run(run_experiment(instance_counts, args.n_steps, args.repetitions))
    aggregated = aggregate(raw_results)

    results_path = os.path.join(args.output_dir, "cache_performance_results.json")
    with open(results_path, "w") as f:
        json.dump(
            {
                "config": {
                    "instance_counts": instance_counts,
                    "n_steps": args.n_steps,
                    "repetitions": args.repetitions,
                },
                "raw_results": raw_results,
                "aggregated": aggregated,
            },
            f,
            indent=4,
        )
    logging.info("Results saved to: %s", results_path)

    save_latency_plot(aggregated, os.path.join(args.output_dir, "cache_latency.png"))
    if not args.skip_compute_plot:
        save_compute_plot(aggregated, os.path.join(args.output_dir, "cache_compute.png"))

    logging.info("Cache performance test complete.")
