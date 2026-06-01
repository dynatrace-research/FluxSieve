import argparse
import csv
import math
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb


# Default dataset locations discovered under /data (1k files x 10k records)
BASELINE_PATTERN = "/data/kstreams-1k-files_10k-records-each/parquet_direct_baseline/parquet/sink/*.parquet"
FLUXSIEVE_PATTERN = "/data/kstreams-1k-files_10k-records-each/parquet_direct_mine/parquet/sink/*.parquet"

DEFAULT_RULES_FILE1 = "streaming-data-plane/kstreams/src/test/resources/random_strings_custom-twofields1.txt"
DEFAULT_RULES_FILE2 = "streaming-data-plane/kstreams/src/test/resources/random_strings_custom-twofields2.txt"


@dataclass(frozen=True)
class QueryDef:
    query_id: int
    name: str
    sql_template: str
    term_source: str
    term_value: str


def percentile(sorted_values: Sequence[float], p: float) -> float:
    if not sorted_values:
        return math.nan
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    idx = (len(sorted_values) - 1) * p
    low = math.floor(idx)
    high = math.ceil(idx)
    if low == high:
        return float(sorted_values[low])
    frac = idx - low
    return float(sorted_values[low] * (1.0 - frac) + sorted_values[high] * frac)


def resolve_file_path(path: str) -> str:
    if os.path.exists(path):
        return path

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

    candidates = [
        os.path.join(repo_root, path),
        os.path.join(os.getcwd(), path),
        os.path.join(script_dir, path),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    raise FileNotFoundError(f"Could not resolve file path: {path}")


def load_terms(file_path: str, max_terms: int) -> List[str]:
    resolved = resolve_file_path(file_path)
    terms: List[str] = []

    with open(resolved, "r", encoding="utf-8") as f:
        for line in f:
            term = line.strip()
            if not term:
                continue
            terms.append(term)
            if len(terms) >= max_terms:
                break

    if not terms:
        raise ValueError(f"No terms loaded from file: {resolved}")

    return terms


def build_baseline_client_queries(
    pattern: str,
    concurrency_level: int,
    terms_file1: List[str],
    terms_file2: List[str],
) -> List[QueryDef]:
    pool: List[Tuple[str, str, str]] = []

    for term in terms_file1:
        esc = term.replace("'", "''")
        pool.append(("file1_content1", term, f"content1 LIKE '%{esc}%'"))

    for term in terms_file2:
        esc = term.replace("'", "''")
        pool.append(("file2_content2", term, f"content2 LIKE '%{esc}%'"))

    if concurrency_level > len(pool):
        raise ValueError(
            f"Not enough unique baseline terms for concurrency={concurrency_level}. "
            f"Available unique predicates={len(pool)}"
        )

    query_defs: List[QueryDef] = []
    for client_id in range(concurrency_level):
        source, raw_term, filt = pool[client_id]
        query_defs.append(
            QueryDef(
                query_id=client_id,
                name=f"baseline-client-{client_id}-{source}",
                sql_template=f"SELECT * FROM read_parquet('{pattern}') WHERE {filt}",
                term_source=source,
                term_value=raw_term,
            )
        )

    return query_defs


def build_fluxsieve_client_queries(
    pattern: str,
    concurrency_level: int,
    rng: random.Random,
) -> List[QueryDef]:
    if concurrency_level > 999:
        raise ValueError("FluxSieve unique IDs are from 1..999; concurrency cannot exceed 999")

    ids = rng.sample(range(1, 1000), concurrency_level)

    query_defs: List[QueryDef] = []
    for client_id, rule_id in enumerate(ids):
        query_defs.append(
            QueryDef(
                query_id=client_id,
                name=f"fluxsieve-client-{client_id}-rule-{rule_id}",
                sql_template=(
                    f"SELECT * FROM read_parquet('{pattern}') "
                    f"WHERE list_contains(matchedRuleIds, {rule_id})"
                ),
                term_source="matchedRuleIds",
                term_value=str(rule_id),
            )
        )

    return query_defs


def setup_connection(duckdb_threads: int, memory_limit: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={duckdb_threads};")
    con.execute(f"SET memory_limit='{memory_limit}';")
    return con


def run_single_thread_warmup(
    query_defs: Sequence[QueryDef],
    warmup_seconds: int,
    duckdb_threads: int,
    memory_limit: str,
) -> None:
    print(f"  Warm-up started ({warmup_seconds}s, single-threaded)")
    con = setup_connection(duckdb_threads, memory_limit)
    try:
        deadline = time.monotonic() + warmup_seconds
        idx = 0
        n = len(query_defs)
        while time.monotonic() < deadline and n > 0:
            q = query_defs[idx % n]
            con.execute(q.sql_template).fetchall()
            idx += 1
    finally:
        con.close()
    print("  Warm-up completed")


class CpuSampler:
    def __init__(self, interval_s: float = 1.0) -> None:
        self.interval_s = interval_s
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.samples: List[float] = []
        self.enabled = False

        try:
            import psutil  # type: ignore

            self.psutil = psutil
            self.enabled = True
        except Exception:
            self.psutil = None
            self.enabled = False

    def start(self) -> None:
        if not self.enabled:
            return

        def _sample() -> None:
            assert self.psutil is not None
            self.psutil.cpu_percent(interval=None)
            while not self._stop_event.is_set():
                val = self.psutil.cpu_percent(interval=self.interval_s)
                self.samples.append(float(val))

        self._thread = threading.Thread(target=_sample, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self.enabled:
            return
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def avg_cpu(self) -> Optional[float]:
        if not self.samples:
            return None
        return float(statistics.mean(self.samples))


def client_loop(
    client_id: int,
    query_defs: Sequence[QueryDef],
    start_event: threading.Event,
    stop_event: threading.Event,
    deadline_monotonic: float,
    system_name: str,
    concurrency_level: int,
    run_id: int,
    duckdb_threads: int,
    memory_limit: str,
    stop_mode: str,
    conn_registry: Dict[int, duckdb.DuckDBPyConnection],
    conn_registry_lock: threading.Lock,
    successful_query_ids: set,
    successful_query_ids_lock: threading.Lock,
) -> List[Dict[str, object]]:
    query = query_defs[client_id]
    con = setup_connection(duckdb_threads, memory_limit)
    rows: List[Dict[str, object]] = []

    with conn_registry_lock:
        conn_registry[client_id] = con

    try:
        start_event.wait()
        while True:
            if stop_event.is_set():
                break

            submit_ts = time.monotonic()
            if submit_ts >= deadline_monotonic:
                break

            t0 = submit_ts
            status = "Success"
            error_msg = ""
            try:
                con.execute(query.sql_template).fetchall()
            except Exception as exc:
                if stop_mode == "strict" and stop_event.is_set():
                    break
                status = "Error"
                error_msg = str(exc)

            t1 = time.monotonic()

            # In strict mode, ignore completions that arrive after deadline.
            if stop_mode == "strict" and t1 > deadline_monotonic:
                break

            if status == "Success":
                with successful_query_ids_lock:
                    successful_query_ids.add(query.query_id)

            rows.append(
                {
                    "concurrency_level": concurrency_level,
                    "system": system_name,
                    "query_id": query.query_id,
                    "query_name": query.name,
                    "client_id": client_id,
                    "run_id": run_id,
                    "term_source": query.term_source,
                    "term_value": query.term_value,
                    "submit_monotonic_s": round(t0, 6),
                    "result_monotonic_s": round(t1, 6),
                    "latency_ms": round((t1 - t0) * 1000.0, 6),
                    "status": status,
                    "error": error_msg,
                }
            )
    finally:
        with conn_registry_lock:
            conn_registry.pop(client_id, None)
        con.close()

    return rows


def interrupt_registered_connections(
    conn_registry: Dict[int, duckdb.DuckDBPyConnection],
    conn_registry_lock: threading.Lock,
) -> None:
    with conn_registry_lock:
        active_connections = list(conn_registry.values())

    for con in active_connections:
        try:
            con.interrupt()
        except Exception:
            pass


def run_one_repetition(
    query_defs: Sequence[QueryDef],
    system_name: str,
    concurrency_level: int,
    run_id: int,
    duration_seconds: int,
    duckdb_threads: int,
    memory_limit: str,
    stop_mode: str,
    ensure_all_queries_once: bool,
    max_extension_seconds: int,
) -> Tuple[List[Dict[str, object]], Optional[float], float]:
    start_event = threading.Event()
    stop_event = threading.Event()
    sampler = CpuSampler(interval_s=1.0)
    rows: List[Dict[str, object]] = []
    conn_registry: Dict[int, duckdb.DuckDBPyConnection] = {}
    conn_registry_lock = threading.Lock()
    successful_query_ids: set = set()
    successful_query_ids_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=concurrency_level) as pool:
        start_monotonic = time.monotonic()
        min_stop_monotonic = start_monotonic + duration_seconds
        max_stop_monotonic = min_stop_monotonic + max_extension_seconds

        futures = [
            pool.submit(
                client_loop,
                i,
                query_defs,
                start_event,
                stop_event,
                max_stop_monotonic,
                system_name,
                concurrency_level,
                run_id,
                duckdb_threads,
                memory_limit,
                stop_mode,
                conn_registry,
                conn_registry_lock,
                successful_query_ids,
                successful_query_ids_lock,
            )
            for i in range(concurrency_level)
        ]

        sampler.start()
        start_event.set()

        target_query_count = len(query_defs)
        while True:
            now = time.monotonic()
            with successful_query_ids_lock:
                completed_query_count = len(successful_query_ids)

            min_duration_reached = now >= min_stop_monotonic
            coverage_reached = completed_query_count >= target_query_count
            hard_limit_reached = now >= max_stop_monotonic

            should_stop = hard_limit_reached or (
                min_duration_reached
                and (
                    (not ensure_all_queries_once)
                    or coverage_reached
                )
            )

            if should_stop:
                stop_event.set()
                if stop_mode == "strict":
                    interrupt_registered_connections(conn_registry, conn_registry_lock)
                break

            time.sleep(0.2)

        for fut in futures:
            rows.extend(fut.result())

    stop_event.set()
    sampler.stop()
    elapsed_s = time.monotonic() - start_monotonic
    return rows, sampler.avg_cpu(), elapsed_s


def summarize_metrics(
    raw_rows: List[Dict[str, object]],
    cpu_rows: List[Dict[str, object]],
    file_layout: str,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    grouped: Dict[Tuple[int, str], List[float]] = {}
    errors: Dict[Tuple[int, str], int] = {}

    for r in raw_rows:
        key = (int(r["concurrency_level"]), str(r["system"]))
        grouped.setdefault(key, [])
        if str(r["status"]) == "Success":
            grouped[key].append(float(r["latency_ms"]))
        else:
            errors[key] = errors.get(key, 0) + 1

    cpu_grouped: Dict[Tuple[int, str], List[float]] = {}
    elapsed_grouped: Dict[Tuple[int, str], float] = {}
    for c in cpu_rows:
        key = (int(c["concurrency_level"]), str(c["system"]))
        avg_cpu = c["avg_cpu_percent"]
        if avg_cpu is not None:
            cpu_grouped.setdefault(key, []).append(float(avg_cpu))
        elapsed_grouped[key] = elapsed_grouped.get(key, 0.0) + float(c["elapsed_seconds"])

    summary_rows: List[Dict[str, object]] = []
    for key in sorted(grouped.keys()):
        concurrency_level, system_name = key
        latencies = sorted(grouped[key])
        n = len(latencies)
        if n == 0:
            p50 = math.nan
            p99 = math.nan
            tail_factor = math.nan
            qps = 0.0
        else:
            p50 = percentile(latencies, 0.50)
            p99 = percentile(latencies, 0.99)
            tail_factor = (p99 / p50) if p50 > 0 else math.nan
            total_measured_time_s = elapsed_grouped.get(key, 0.0)
            qps = (n / total_measured_time_s) if total_measured_time_s > 0 else 0.0

        avg_cpu_percent = None
        if key in cpu_grouped and cpu_grouped[key]:
            avg_cpu_percent = round(statistics.mean(cpu_grouped[key]), 4)

        summary_rows.append(
            {
                "concurrency_level": concurrency_level,
                "system": system_name,
                "file_layout": file_layout,
                "num_successful_queries": n,
                "num_errors": errors.get(key, 0),
                "median_latency_ms": round(p50, 6) if not math.isnan(p50) else "",
                "p99_latency_ms": round(p99, 6) if not math.isnan(p99) else "",
                "tail_factor_p99_over_median": round(tail_factor, 6) if not math.isnan(tail_factor) else "",
                "qps": round(qps, 6),
                "total_elapsed_seconds": round(elapsed_grouped.get(key, 0.0), 6),
                "avg_cpu_percent": avg_cpu_percent if avg_cpu_percent is not None else "",
            }
        )

    by_query_grouped: Dict[Tuple[int, str, int, str, str, str], List[float]] = {}
    for r in raw_rows:
        if str(r["status"]) != "Success":
            continue
        key = (
            int(r["concurrency_level"]),
            str(r["system"]),
            int(r["query_id"]),
            str(r["query_name"]),
            str(r["term_source"]),
            str(r["term_value"]),
        )
        by_query_grouped.setdefault(key, []).append(float(r["latency_ms"]))

    by_query_rows: List[Dict[str, object]] = []
    for key in sorted(by_query_grouped.keys()):
        concurrency_level, system_name, query_id, query_name, term_source, term_value = key
        vals = sorted(by_query_grouped[key])
        p50 = percentile(vals, 0.50)
        p99 = percentile(vals, 0.99)
        tail = (p99 / p50) if p50 > 0 else math.nan
        by_query_rows.append(
            {
                "concurrency_level": concurrency_level,
                "system": system_name,
                "file_layout": file_layout,
                "query_id": query_id,
                "query_name": query_name,
                "term_source": term_source,
                "term_value": term_value,
                "num_successful_queries": len(vals),
                "median_latency_ms": round(p50, 6),
                "p99_latency_ms": round(p99, 6),
                "tail_factor_p99_over_median": round(tail, 6) if not math.isnan(tail) else "",
            }
        )

    return summary_rows, by_query_rows


def write_csv(path: str, rows: List[Dict[str, object]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Concurrent DuckDB benchmark with unique per-client terms: "
            "Baseline from rules TXT files and FluxSieve from unique random rule IDs"
        )
    )
    parser.add_argument("--scenario", default="10M-concurrency-benchmark-unique-terms")
    parser.add_argument("--file-layout", default="~1k-files_x_10k-records")
    parser.add_argument("--concurrency-levels", default="8,16,32")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument(
        "--ensure-all-queries-once",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After min duration, continue until each query ID has at least one successful completion.",
    )
    parser.add_argument(
        "--max-extension-seconds",
        type=int,
        default=300,
        help="Safety cap for additional runtime beyond --duration-seconds when coverage is not met.",
    )
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--idle-between-runs-seconds", type=int, default=60)
    parser.add_argument("--duckdb-threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--stop-mode", choices=["soft", "strict"], default="soft")
    parser.add_argument("--baseline-pattern", default=BASELINE_PATTERN)
    parser.add_argument("--fluxsieve-pattern", default=FLUXSIEVE_PATTERN)
    parser.add_argument("--baseline-terms-file1", default=DEFAULT_RULES_FILE1)
    parser.add_argument("--baseline-terms-file2", default=DEFAULT_RULES_FILE2)
    parser.add_argument("--baseline-max-terms-file1", type=int, default=800)
    parser.add_argument("--baseline-max-terms-file2", type=int, default=200)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--output-dir", default="results")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_dir = os.path.join(args.output_dir, f"{args.scenario}_{now}")
    os.makedirs(result_dir, exist_ok=True)

    levels = [int(x.strip()) for x in args.concurrency_levels.split(",") if x.strip()]
    rng = random.Random(args.random_seed)

    baseline_terms1 = load_terms(args.baseline_terms_file1, args.baseline_max_terms_file1)
    baseline_terms2 = load_terms(args.baseline_terms_file2, args.baseline_max_terms_file2)

    print("=" * 80)
    print("Concurrency Benchmark: DuckDB Baseline vs FluxSieve (Unique Client Terms)")
    print("=" * 80)
    print(f"Scenario: {args.scenario}")
    print(f"File layout: {args.file_layout}")
    print(f"Concurrency levels: {levels}")
    print(f"Repetitions: {args.repetitions}")
    print(f"Min duration per run: {args.duration_seconds}s")
    print(f"Ensure all queries once: {args.ensure_all_queries_once}")
    print(f"Max extension per run: {args.max_extension_seconds}s")
    print(f"Warm-up: {args.warmup_seconds}s (single-threaded)")
    print(f"Idle between repetitions: {args.idle_between_runs_seconds}s")
    print(f"DuckDB threads per connection: {args.duckdb_threads}")
    print(f"DuckDB memory_limit: {args.memory_limit}")
    print(f"Stop mode: {args.stop_mode}")
    print(f"Baseline terms file1 loaded: {len(baseline_terms1)}")
    print(f"Baseline terms file2 loaded: {len(baseline_terms2)}")
    print(f"FluxSieve random seed: {args.random_seed}")
    print(f"Output: {result_dir}")
    print("=" * 80)

    raw_rows: List[Dict[str, object]] = []
    cpu_rows: List[Dict[str, object]] = []

    for concurrency_level in levels:
        system_defs: List[Tuple[str, List[QueryDef]]] = [
            (
                "BASELINE",
                build_baseline_client_queries(
                    pattern=args.baseline_pattern,
                    concurrency_level=concurrency_level,
                    terms_file1=baseline_terms1,
                    terms_file2=baseline_terms2,
                ),
            ),
            (
                "FLUXSIEVE",
                build_fluxsieve_client_queries(
                    pattern=args.fluxsieve_pattern,
                    concurrency_level=concurrency_level,
                    rng=rng,
                ),
            ),
        ]

        for system_name, query_defs in system_defs:
            print("\n" + "-" * 80)
            print(f"System={system_name} | Concurrency={concurrency_level}")
            print("-" * 80)

            run_single_thread_warmup(
                query_defs=query_defs,
                warmup_seconds=args.warmup_seconds,
                duckdb_threads=args.duckdb_threads,
                memory_limit=args.memory_limit,
            )

            for run_id in range(1, args.repetitions + 1):
                print(f"  Repetition {run_id}/{args.repetitions} started")
                rep_rows, avg_cpu, elapsed = run_one_repetition(
                    query_defs=query_defs,
                    system_name=system_name,
                    concurrency_level=concurrency_level,
                    run_id=run_id,
                    duration_seconds=args.duration_seconds,
                    duckdb_threads=args.duckdb_threads,
                    memory_limit=args.memory_limit,
                    stop_mode=args.stop_mode,
                    ensure_all_queries_once=args.ensure_all_queries_once,
                    max_extension_seconds=args.max_extension_seconds,
                )

                raw_rows.extend(rep_rows)
                cpu_rows.append(
                    {
                        "concurrency_level": concurrency_level,
                        "system": system_name,
                        "run_id": run_id,
                        "avg_cpu_percent": round(avg_cpu, 4) if avg_cpu is not None else None,
                        "elapsed_seconds": round(elapsed, 4),
                    }
                )

                success_count = sum(1 for r in rep_rows if r["status"] == "Success")
                error_count = len(rep_rows) - success_count
                print(
                    f"  Repetition {run_id}/{args.repetitions} done: "
                    f"queries={len(rep_rows)} success={success_count} errors={error_count} "
                    f"avg_cpu={avg_cpu if avg_cpu is not None else 'N/A'}"
                )

                if run_id < args.repetitions:
                    print(f"  Idle {args.idle_between_runs_seconds}s before next repetition")
                    time.sleep(args.idle_between_runs_seconds)

    summary_rows, by_query_rows = summarize_metrics(
        raw_rows=raw_rows,
        cpu_rows=cpu_rows,
        file_layout=args.file_layout,
    )

    raw_csv = os.path.join(result_dir, "raw_latencies.csv")
    cpu_csv = os.path.join(result_dir, "cpu_per_run.csv")
    summary_csv = os.path.join(result_dir, "summary_overall.csv")
    summary_query_csv = os.path.join(result_dir, "summary_by_query.csv")

    write_csv(raw_csv, raw_rows)
    write_csv(cpu_csv, cpu_rows)
    write_csv(summary_csv, summary_rows)
    write_csv(summary_query_csv, by_query_rows)

    print("\n" + "=" * 80)
    print("Benchmark Completed")
    print("=" * 80)
    print(f"Raw latencies: {raw_csv}")
    print(f"CPU per run:   {cpu_csv}")
    print(f"Summary:       {summary_csv}")
    print(f"By query:      {summary_query_csv}")


if __name__ == "__main__":
    main()
