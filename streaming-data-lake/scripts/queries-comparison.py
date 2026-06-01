# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import duckdb
import time
import pandas as pd
import os
import subprocess
import sys
import glob
from datetime import datetime
import pyarrow.feather as feather
import pyarrow as pa

# ====================================
# Configuration
# ====================================
scenario_name = "10M-unified_benchmark_parquet_zstd"
CLEAR_CACHE = True
NUM_ITERATIONS_COLD = 10
NUM_ITERATIONS_HOT = 10
THREADS = 1
MEMORY_LIMIT = "4GB"

# Test only parquet format
FORMATS = ["parquet"]

# Copy output configuration
COPY_FORMAT = os.getenv("COPY_FORMAT", "parquet").lower()
COPY_PARQUET_COMPRESSION = os.getenv("COPY_PARQUET_COMPRESSION", "zstd")

# Base fields always included
base_fields = ["timestamp", "content1", "content2", "status", "eventType"]

# ====================================
# Logging setup
# ====================================
class TeeIO:
    def __init__(self, stream, log_file_handle):
        self.stream = stream
        self.log = log_file_handle

    def write(self, data):
        self.stream.write(data)
        try:
            self.log.write(data)
        except Exception:
            pass

    def flush(self):
        try:
            self.stream.flush()
        except Exception:
            pass
        try:
            self.log.flush()
        except Exception:
            pass

def setup_logging(scenario):
    results_base_dir = os.path.join("results", scenario)
    os.makedirs(results_base_dir, exist_ok=True)
    log_path = os.path.join(results_base_dir, f"{scenario}.log")
    log_fh = open(log_path, "a", buffering=1, encoding="utf-8")
    sys.stdout = TeeIO(sys.__stdout__, log_fh)
    sys.stderr = TeeIO(sys.__stderr__, log_fh)
    return results_base_dir, log_path

# ====================================
# Path helpers
# ====================================
def baseline_pattern():
    """Path pattern for baseline LIKE queries"""
    return "/data/kstreams-1k-files_10k-records-each/parquet_direct_baseline/parquet/sink/*.parquet"

def array_pattern():
    """Path pattern for array-based enrichment"""
    return "/data/kstreams-1k-files_10k-records-each/parquet_direct_mine/parquet/sink/*.parquet"

# ====================================
# Query definitions
# ====================================
BASELINE_QUERIES = [
    {
        "comment": "Q1-baseline-like",
        "filter": "content1 LIKE '%melborpmelborp%'",
        "copy_file": "baseline_q1",
        "approach": "BASELINE"
    },
    {
        "comment": "Q2-baseline-like",
        "filter": "content1 LIKE '%problem_id%'",
        "copy_file": "baseline_q2",
        "approach": "BASELINE"
    },
    {
        "comment": "Q3-baseline-like",
        "filter": "content1 LIKE '%Error_999%'",
        "copy_file": "baseline_q3",
        "approach": "BASELINE"
    },
    {
        "comment": "Q4-baseline-like",
        "filter": "content1 LIKE '%problem_id%' AND content2 LIKE '%123456789%'",
        "copy_file": "baseline_q4",
        "approach": "BASELINE"
    },
]


ARRAY_QUERIES = [
    {
        "comment": "Q1-array-single-rule",
        "filter": "list_contains(matchedRuleIds, 1)",
        "copy_file": "array_q1",
        "approach": "ARRAY",
        "rule_ids": [1]
    },
    {
        "comment": "Q2-array-single-rule",
        "filter": "list_contains(matchedRuleIds, 201)",
        "copy_file": "array_q2",
        "approach": "ARRAY",
        "rule_ids": [201]
    },
    {
        "comment": "Q3-array-single-rule",
        "filter": "list_contains(matchedRuleIds, 401)",
        "copy_file": "array_q3",
        "approach": "ARRAY",
        "rule_ids": [401]
    },
    {
        "comment": "Q4-array-two-rules-AND",
        "filter": "list_contains(matchedRuleIds, 799) AND list_contains(matchedRuleIds, 999)",
        "copy_file": "array_q4",
        "approach": "ARRAY",
        "rule_ids": [799, 999]
    },
]

# ====================================
# Utilities
# ====================================
def _sanitize_filename(dirpath, filename_base, ext):
    return os.path.join(dirpath, f"{filename_base}.{ext}")

def clear_system_cache():
    if not CLEAR_CACHE:
        return
    try:
        subprocess.run(['sudo', 'sync'], check=True, capture_output=True)
        subprocess.run(['sudo', 'sh', '-c', 'echo 3 > /proc/sys/vm/drop_caches'], check=True, capture_output=True)
        print("✓ System cache cleared")
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to clear cache: {e}")
    except FileNotFoundError:
        print("✗ sudo not found - continuing without cache clear")

def get_files_info(pattern):
    files = glob.glob(pattern)
    if not files:
        return 0, 0.0
    total_size = sum(os.path.getsize(f) for f in files) / (1024**3)
    return len(files), total_size

def setup_connection():
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS};")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}';")
    return con

def build_projection(approach, streaming_rules=None, include_array=False):
    """Build projection list based on approach"""
    cols = list(base_fields)
    if approach in ["BASELINE-STREAMING"] and streaming_rules:
        cols += streaming_rules
    elif approach == "ARRAY" or include_array:
        cols.append("matchedRuleIds")
    seen = set()
    proj = []
    for c in cols:
        if c not in seen:
            proj.append(c)
            seen.add(c)
    return proj

# ====================================
# Measurement functions
# ====================================
def measure_count(con, pattern, filter_expr, comment):
    start = time.perf_counter()
    try:
        sql = f"SELECT COUNT(*) AS cnt FROM read_parquet('{pattern}') WHERE {filter_expr}"
        cnt = con.execute(sql).fetchone()[0]
        status = "Success"
    except Exception as e:
        cnt = None
        status = f"Error: {e}"

    end = time.perf_counter()
    time_ms = (end - start) * 1000
    print(f"  COUNT {comment}: {time_ms:.2f} ms [{status}] rows={cnt}")
    return cnt, time_ms, status

def measure_copy(con, pattern, filter_expr, out_path_base, comment, projection_cols):
    if COPY_FORMAT == "parquet":
        out_ext = "parquet"
    elif COPY_FORMAT == "arrow":
        out_ext = "feather"
    else:
        raise ValueError(f"Unsupported COPY_FORMAT: {COPY_FORMAT}")

    results_dir = os.path.dirname(out_path_base)
    os.makedirs(results_dir, exist_ok=True)
    out_path = _sanitize_filename(results_dir, os.path.basename(out_path_base), out_ext)

    if os.path.exists(out_path):
        try:
            os.remove(out_path)
        except Exception:
            base, ext = os.path.splitext(out_path)
            out_path = f"{base}_{int(time.time()*1000)}{ext}"

    select_list = ", ".join([f'"{c}"' for c in projection_cols]) if projection_cols else "*"

    start = time.perf_counter()
    status = "Unknown"
    try:
        if COPY_FORMAT == "parquet":
            compression_opt = COPY_PARQUET_COMPRESSION
            copy_sql = f"""
            COPY (
              SELECT {select_list}
              FROM read_parquet('{pattern}')
              WHERE {filter_expr}
            ) TO '{out_path}' (FORMAT 'parquet', COMPRESSION '{compression_opt}');
            """
        else:
            copy_sql = f"""
            COPY (
              SELECT {select_list}
              FROM read_parquet('{pattern}')
              WHERE {filter_expr}
            ) TO '{out_path}' (FORMAT 'ipc');
            """
        con.execute(copy_sql)
        status = "Success"
    except Exception as e:
        status = f"Error: {e}"

    end = time.perf_counter()
    time_ms = (end - start) * 1000

    size_bytes = os.path.getsize(out_path) if (status == "Success" and os.path.exists(out_path)) else 0

    rows_copied = None
    if status == "Success":
        try:
            if COPY_FORMAT == "parquet":
                rows_copied = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
            else:
                table = feather.read_table(out_path)
                rows_copied = len(table)
        except Exception as e:
            rows_copied = None
            status = f"Success with readback error: {e}"

    print(f"  COPY  {comment}: {time_ms:.2f} ms [{status}] rows_copied={rows_copied} size={size_bytes/1024/1024:.2f} MB")
    return rows_copied, time_ms, status, size_bytes

# ====================================
# Metrics recording
# ====================================
def append_metric(target_list, phase, iteration, approach, comment, pattern, filter_expr, 
                 projection_cols, cnt, cnt_ms, cnt_status, rows_copied, copy_ms, 
                 copy_status, out_bytes, extra_info=""):
    target_list.append({
        "Phase": phase,
        "Iteration": iteration,
        "Approach": approach,
        "Comment": comment,
        "Pattern": pattern,
        "Filter": filter_expr,
        "ProjectionCols": ",".join(projection_cols),
        "ExtraInfo": extra_info,
        "CountRows": cnt,
        "CountTimeMs": cnt_ms,
        "CountStatus": cnt_status,
        "CopyRows": rows_copied,
        "CopyTimeMs": copy_ms,
        "CopyStatus": copy_status,
        "CopyOutBytes": out_bytes,
        "CopyFormat": COPY_FORMAT,
        "CopyCompression": COPY_PARQUET_COMPRESSION if COPY_FORMAT == "parquet" else ""
    })

# ====================================
# Benchmark execution functions
# ====================================
def run_benchmark(queries, pattern, approach_name, results_base_dir, cold_metrics, hot_metrics):
    """Generic benchmark runner for any query set"""
    try:
        num_files, size_gb = get_files_info(pattern)
        print(f"\n{approach_name}: {num_files} files, {size_gb:.2f} GB")
        print(f"Pattern: '{pattern}'")
    except Exception as e:
        print(f"Error reading files for {approach_name}: {e}")
        return

    out_data_dir = os.path.join(results_base_dir, "data")
    os.makedirs(out_data_dir, exist_ok=True)

    # COLD benchmarks
    print(f"\n[COLD] Running {NUM_ITERATIONS_COLD} iterations - {approach_name}")
    print("=" * 70)
    for iteration in range(NUM_ITERATIONS_COLD):
        print(f"\n--- Cold Iteration {iteration + 1}/{NUM_ITERATIONS_COLD} ---")
        for q in queries:
            comment = q["comment"]
            filter_expr = q["filter"]
            copy_file_base = q["copy_file"]
            approach = q["approach"]
            
            streaming_rules = q.get("streaming_rules", [])
            rule_ids = q.get("rule_ids", [])
            extra_info = ",".join(map(str, rule_ids)) if rule_ids else ""

            clear_system_cache()
            time.sleep(0.5)
            con = setup_connection()

            cnt, cnt_ms, cnt_status = measure_count(con, pattern, filter_expr, comment)

            clear_system_cache()
            time.sleep(0.5)

            projection_cols = build_projection(approach, streaming_rules, include_array=(approach == "ARRAY"))
            out_path_base = os.path.join(out_data_dir, f"cold_{approach}_{copy_file_base}_iter{iteration}")
            rows_copied, copy_ms, copy_status, out_bytes = measure_copy(
                con, pattern, filter_expr, out_path_base, comment, projection_cols
            )

            con.close()

            append_metric(cold_metrics, "COLD", iteration + 1, approach, f"{comment}-COUNT",
                        pattern, filter_expr, projection_cols,
                        cnt, cnt_ms, cnt_status, None, None, None, None, extra_info)

            append_metric(cold_metrics, "COLD", iteration + 1, approach, f"{comment}-COPY",
                        pattern, filter_expr, projection_cols,
                        None, None, None, rows_copied, copy_ms, copy_status, out_bytes, extra_info)

    # HOT benchmarks
    print(f"\n[HOT] Running {NUM_ITERATIONS_HOT} iterations - {approach_name}")
    print("=" * 70)
    con = setup_connection()
    
    print("Warming up cache...")
    for q in queries:
        filter_expr = q["filter"]
        try:
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{pattern}') WHERE {filter_expr}").fetchone()
        except Exception as e:
            print(f"  Warm-up warning for '{filter_expr[:50]}...': {e}")
    print("✓ Warm-up complete\n")

    for iteration in range(NUM_ITERATIONS_HOT):
        print(f"  Iteration {iteration + 1}/{NUM_ITERATIONS_HOT}")
        for q in queries:
            comment = q["comment"]
            filter_expr = q["filter"]
            copy_file_base = q["copy_file"]
            approach = q["approach"]
            
            streaming_rules = q.get("streaming_rules", [])
            rule_ids = q.get("rule_ids", [])
            extra_info = ",".join(map(str, rule_ids)) if rule_ids else ""

            cnt, cnt_ms, cnt_status = measure_count(con, pattern, filter_expr, comment)

            projection_cols = build_projection(approach, streaming_rules, include_array=(approach == "ARRAY"))
            out_path_base = os.path.join(out_data_dir, f"hot_{approach}_{copy_file_base}_iter{iteration}")
            rows_copied, copy_ms, copy_status, out_bytes = measure_copy(
                con, pattern, filter_expr, out_path_base, comment, projection_cols
            )

            append_metric(hot_metrics, "HOT", iteration + 1, approach, f"{comment}-COUNT",
                        pattern, filter_expr, projection_cols,
                        cnt, cnt_ms, cnt_status, None, None, None, None, extra_info)

            append_metric(hot_metrics, "HOT", iteration + 1, approach, f"{comment}-COPY",
                        pattern, filter_expr, projection_cols,
                        None, None, None, rows_copied, copy_ms, copy_status, out_bytes, extra_info)

    con.close()

# ====================================
# Main
# ====================================
def main():
    results_base_dir, log_path = setup_logging(scenario_name)

    print("=" * 70)
    print("DuckDB Unified Benchmark: Baseline vs Array-Based")
    print("=" * 70)
    print(f"Scenario: {scenario_name}")
    print(f"Threads: {THREADS}, Memory: {MEMORY_LIMIT}, Clear cache: {CLEAR_CACHE}")
    print(f"Cold iterations: {NUM_ITERATIONS_COLD}, Hot iterations: {NUM_ITERATIONS_HOT}")
    print(f"COPY format: {COPY_FORMAT}, Compression: {COPY_PARQUET_COMPRESSION}")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Log: {log_path}")
    print(f"Results: {results_base_dir}")
    print("=" * 70)

    cold_metrics = []
    hot_metrics = []

    print("\n[1/4] Checking input files...")
    baseline_pat = baseline_pattern()
    array_pat = array_pattern()
    
    try:
        num_files, size_gb = get_files_info(baseline_pat)
        print(f"  Baseline: {num_files} files, {size_gb:.2f} GB")
    except Exception as e:
        print(f"  Baseline: Error - {e}")
    
    try:
        num_files, size_gb = get_files_info(array_pat)
        print(f"  Array: {num_files} files, {size_gb:.2f} GB")
    except Exception as e:
        print(f"  Array: Error - {e}")

    print("\n[2/4] Running Baseline LIKE benchmarks...")
    run_benchmark(BASELINE_QUERIES, baseline_pat, "BASELINE-LIKE", results_base_dir, cold_metrics, hot_metrics)

    # print("\n[2.5/4] Running Baseline Streaming benchmarks...")
    # run_benchmark(BASELINE_STREAMING_QUERIES, baseline_pat, "BASELINE-STREAMING", results_base_dir, cold_metrics, hot_metrics)

    print("\n[3/4] Running Array-based benchmarks...")
    run_benchmark(ARRAY_QUERIES, array_pat, "ARRAY", results_base_dir, cold_metrics, hot_metrics)

    print("\n[4/4] Saving results...")
    print("=" * 70)
    cold_df = pd.DataFrame(cold_metrics)
    hot_df = pd.DataFrame(hot_metrics)

    print(f"  Cold metrics: {len(cold_df)} rows")
    print(f"  Hot metrics:  {len(hot_df)} rows")

    cold_csv = os.path.join(results_base_dir, "cold_metrics_unified.csv")
    hot_csv = os.path.join(results_base_dir, "hot_metrics_unified.csv")
    cold_df.to_csv(cold_csv, index=False)
    hot_df.to_csv(hot_csv, index=False)
    print(f"Saved: {cold_csv}")
    print(f"Saved: {hot_csv}")

    print("\n" + "=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)

    print("\n--- Cold COUNT (ms) ---")
    if len(cold_df) > 0:
        cold_count = cold_df[cold_df['Comment'].str.contains('COUNT')]
        if len(cold_count) > 0:
            summary = cold_count.groupby(['Approach', 'Comment'])['CountTimeMs'].agg(['mean', 'std', 'min', 'max', 'count']).round(2)
            print(summary.to_string())

    print("\n--- Cold COPY (ms) ---")
    if len(cold_df) > 0:
        cold_copy = cold_df[cold_df['Comment'].str.contains('COPY')]
        if len(cold_copy) > 0:
            summary = cold_copy.groupby(['Approach', 'Comment'])['CopyTimeMs'].agg(['mean', 'std', 'min', 'max', 'count']).round(2)
            print(summary.to_string())

    print("\n--- Hot COUNT (ms) ---")
    if len(hot_df) > 0:
        hot_count = hot_df[hot_df['Comment'].str.contains('COUNT')]
        if len(hot_count) > 0:
            summary = hot_count.groupby(['Approach', 'Comment'])['CountTimeMs'].agg(['mean', 'std', 'min', 'max', 'count']).round(2)
            print(summary.to_string())

    print("\n--- Hot COPY (ms) ---")
    if len(hot_df) > 0:
        hot_copy = hot_df[hot_df['Comment'].str.contains('COPY')]
        if len(hot_copy) > 0:
            summary = hot_copy.groupby(['Approach', 'Comment'])['CopyTimeMs'].agg(['mean', 'std', 'min', 'max', 'count']).round(2)
            print(summary.to_string())

    print("\n--- Data Consistency Check ---")
    for df, phase in [(cold_df, "COLD"), (hot_df, "HOT")]:
        if len(df) == 0:
            continue
        safe = df.fillna({"CountStatus": "", "CopyStatus": ""})
        ok = (safe["CountStatus"] == "Success") & (safe["CopyStatus"].astype(str).str.startswith("Success"))
        inconsistent = df[ok & (df["CountRows"] != df["CopyRows"])]
        if len(inconsistent) == 0:
            print(f"  {phase}: ✓ All queries consistent")
        else:
            print(f"  {phase}: ⚠ Inconsistencies detected:")
            for _, row in inconsistent.iterrows():
                print(f"    {row['Approach']} {row['Comment']}: count={row['CountRows']} copy={row['CopyRows']}")

    print("\n--- Approach Comparison (Hot Average) ---")
    if len(hot_df) > 0:
        hot_count_avg = hot_df[hot_df['Comment'].str.contains('COUNT')].groupby('Approach')['CountTimeMs'].mean().round(2)
        hot_copy_avg = hot_df[hot_df['Comment'].str.contains('COPY')].groupby('Approach')['CopyTimeMs'].mean().round(2)
        print("\nAverage COUNT time (ms) by approach:")
        print(hot_count_avg.to_string())
        print("\nAverage COPY time (ms) by approach:")
        print(hot_copy_avg.to_string())

    print("\n" + "=" * 70)
    print("Benchmark completed successfully!")
    print(f"Finished: {datetime.now().isoformat()}")
    print("=" * 70)

if __name__ == "__main__":
    main()
