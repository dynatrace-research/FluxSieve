# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import re
import os

# ==========================
# Configuration
# ==========================
# Each entry: (folder, x-axis label). Order is "worst" -> "best".
CONFIGS = [
    ("final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism1_~5k-parquet-files_~2k-records-each",
     r"1 thread\\$\sim$5k$\times$2k"),
    ("final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism1_~1k-parquet-files_~10k-records-each",
     r"1 thread\\$\sim$1k$\times$10k"),
    ("final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism4_~5k-parquet-files_~2k-records-each",
     r"4 threads\\$\sim$5k$\times$2k"),
    ("final-10M-records_ultra-high-selectivity_parquet_zstd-10reps_parallelism4_~1k-parquet-files_~10k-records-each",
     r"4 threads\\$\sim$1k$\times$10k"),
]
SCENARIO_NAME = "10M-unified_benchmark_parquet_zstd"
COLD_NAME = "cold_metrics_unified.csv"
HOT_NAME = "hot_metrics_unified.csv"

QUERIES_OF_INTEREST = [1, 2, 3, 4]

# Raw CSV values
BASELINE_RAW = "BASELINE"
ARRAY_RAW = "ARRAY"

# Display labels (match Pinot UHSQ style)
BASELINE_DISPLAY = "Baseline: full scan"
ARRAY_DISPLAY = "Our approach: FluxSieve"

QUERY_LABELS = {
    "Q1": "Q1 - Single cond. filter (no matches)",
    "Q2": "Q2 - Single cond. filter (ultra rare matches)",
    "Q3": "Q3 - Single cond. filter and aggregation",
    "Q4": "Q4 - Multi column filter (two cond.)",
}

# Match Pinot rcParams for visual consistency with the paper
plt.rcParams['figure.figsize'] = [3.33, 2.22]
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['font.size'] = 19
plt.rcParams['font.family'] = 'serif'
plt.rcParams['axes.grid'] = True
plt.rcParams['axes.grid.axis'] = 'y'
plt.rcParams['grid.color'] = 'dddddd'
plt.rcParams['axes.axisbelow'] = True
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r"""
        \usepackage{libertine}
        \usepackage[libertine]{newtxmath}
        """

N_BOOT = 10_000
ALPHA = 0.05
RNG = np.random.default_rng(42)


def bootstrap_percentile_ci_median(values, alpha=0.05, n_boot=10_000, rng=None):
    vals = np.asarray(values, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return np.nan, np.nan, np.nan
    med = np.median(vals)
    if vals.size == 1:
        return med, med, med
    rng = rng or np.random.default_rng()
    idx = rng.integers(0, vals.size, size=(n_boot, vals.size))
    boot_meds = np.median(vals[idx], axis=1)
    ci_low = np.quantile(boot_meds, alpha / 2)
    ci_high = np.quantile(boot_meds, 1 - alpha / 2)
    return med, ci_low, ci_high


def parse_qid_and_op(comment: str):
    text = str(comment)
    m = re.match(r"^(Q\d+)", text)
    if not m:
        return None, None
    qid = m.group(1)
    if "-COUNT" in text:
        return qid, "COUNT"
    if "-COPY" in text:
        return qid, "COPY"
    return qid, None


def load_one_config(folder):
    base = os.path.join(os.path.dirname(__file__), folder, "results", SCENARIO_NAME)
    cold = pd.read_csv(os.path.join(base, COLD_NAME))
    hot = pd.read_csv(os.path.join(base, HOT_NAME))
    cold["Type"] = "cold runs"
    hot["Type"] = "hot runs"
    df = pd.concat([cold, hot], ignore_index=True)
    df = df[(df["CountStatus"] == "Success") | (df["CopyStatus"] == "Success")].copy()
    df[["QueryID", "OpType"]] = df["Comment"].apply(lambda c: pd.Series(parse_qid_and_op(c)))
    df["TimeUsedMs"] = df.apply(
        lambda r: r["CountTimeMs"] if r["OpType"] == "COUNT" else r["CopyTimeMs"], axis=1
    )
    df = df.dropna(subset=["QueryID", "OpType", "TimeUsedMs"])
    return df


# ==========================
# Aggregate stats across configs
# ==========================
records = []
for cfg_idx, (folder, _label) in enumerate(CONFIGS):
    df = load_one_config(folder)
    for qid_num in QUERIES_OF_INTEREST:
        qid = f"Q{qid_num}"
        for op in ("COUNT", "COPY"):
            for approach in (BASELINE_RAW, ARRAY_RAW):
                for run_type in ("cold runs", "hot runs"):
                    sub = df[
                        (df["QueryID"] == qid)
                        & (df["OpType"] == op)
                        & (df["Approach"] == approach)
                        & (df["Type"] == run_type)
                    ]["TimeUsedMs"]
                    med, lo, hi = bootstrap_percentile_ci_median(
                        sub.values, alpha=ALPHA, n_boot=N_BOOT, rng=RNG
                    )
                    records.append({
                        "ConfigIdx": cfg_idx,
                        "Query": qid_num,
                        "Op": op,
                        "Approach": approach,
                        "Type": run_type,
                        "Median": med,
                        "CI_low": lo,
                        "CI_high": hi,
                    })

stats_df = pd.DataFrame(records)


# ==========================
# Speedups: BASELINE / ARRAY for cold and hot, per query/config/op
# ==========================
speedup_rows = []
for qid_num in QUERIES_OF_INTEREST:
    for op in ("COUNT", "COPY"):
        for cfg_idx in range(len(CONFIGS)):
            sel = stats_df[(stats_df["Query"] == qid_num) & (stats_df["Op"] == op) & (stats_df["ConfigIdx"] == cfg_idx)]
            su = {}
            for run_type in ("cold runs", "hot runs"):
                base = sel[(sel["Approach"] == BASELINE_RAW) & (sel["Type"] == run_type)]["Median"].values
                arr = sel[(sel["Approach"] == ARRAY_RAW) & (sel["Type"] == run_type)]["Median"].values
                v = np.nan
                if base.size and arr.size and np.isfinite(base[0]) and np.isfinite(arr[0]) and arr[0] != 0:
                    v = base[0] / arr[0]
                su[run_type] = v
            speedup_rows.append({
                "Query": qid_num, "Op": op, "ConfigIdx": cfg_idx,
                "SpeedupCold": su["cold runs"], "SpeedupHot": su["hot runs"],
            })
speedup_df = pd.DataFrame(speedup_rows)


def fmt_no_zero(x, pos):
    return f'{x:g}'


no_zero_formatter = FuncFormatter(fmt_no_zero)


# Per-(query, op) tick values to drop, for high-range subplots where the
# auto-picker keeps low-range ticks (5, 15, 25) that are visually crowded
# at the bottom of the axis.
TICK_DROPS = {
    (4, "COPY"):  [5, 15, 25],
    (2, "COUNT"): [5, 15, 25],
    (3, "COUNT"): [5, 15, 25],
    (4, "COUNT"): [5, 15, 25],
}


def speedup_ticks(y2_top, min_gap_frac=0.035):
    """Adaptive speedup-axis ticks. Walks a dense ladder of round numbers and
    keeps each candidate only if it is at least `min_gap_frac` of the axis
    range away from the previously kept tick, so labels never collide. Always
    includes 1 (used as the speedup=1 reference)."""
    ladder = [1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200,
              250, 300, 400, 500, 750, 1000, 1500, 2000, 3000, 5000]
    fit = [t for t in ladder if t <= y2_top]
    if not fit:
        return [1] if y2_top >= 0.5 else []
    if fit[0] != 1:
        fit = [1] + fit
    min_gap = y2_top * min_gap_frac
    kept = [fit[0]]
    for t in fit[1:]:
        if t - kept[-1] >= min_gap:
            kept.append(t)
    return kept


# ==========================
# Plot one figure per operation
# ==========================
colors = {
    (BASELINE_RAW, "cold runs"): "#ff7f0e",
    (BASELINE_RAW, "hot runs"): "#ff7f0e",
    (ARRAY_RAW, "cold runs"): "#1f77b4",
    (ARRAY_RAW, "hot runs"): "#1f77b4",
}
linestyles = {"cold runs": "-", "hot runs": "--"}
x_positions = list(range(len(CONFIGS)))
x_labels = [lbl for _, lbl in CONFIGS]


def make_plot(op, output_basename):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    axes = axes.flatten()

    legend_handles = []

    for i, qid_num in enumerate(QUERIES_OF_INTEREST):
        ax = axes[i]
        df_q = stats_df[(stats_df["Query"] == qid_num) & (stats_df["Op"] == op)]

        # Plot 4 lines: 2 approaches × 2 run types
        for approach in (BASELINE_RAW, ARRAY_RAW):
            for run_type in ("cold runs", "hot runs"):
                grp = df_q[(df_q["Approach"] == approach) & (df_q["Type"] == run_type)].sort_values("ConfigIdx")
                if grp.empty:
                    continue
                positions = grp["ConfigIdx"].tolist()
                approach_label = BASELINE_DISPLAY if approach == BASELINE_RAW else ARRAY_DISPLAY
                label = f"{approach_label} ({run_type})"
                line = ax.errorbar(
                    positions, grp["Median"],
                    yerr=[grp["Median"] - grp["CI_low"], grp["CI_high"] - grp["Median"]],
                    label=label,
                    color=colors[(approach, run_type)],
                    linestyle=linestyles[run_type],
                    marker="o", capsize=4,
                )
                if i == 0:
                    legend_handles.append(line)

        ax.set_title(QUERY_LABELS.get(f"Q{qid_num}", f"Query {qid_num}"))
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels)
        ax.set_ylabel(r"Median time (ms) [95\% CI]")
        if i >= 2:
            ax.set_xlabel("Configuration (parallelism / files $\\times$ records)")
        ax.yaxis.set_major_formatter(no_zero_formatter)

        # Right axis: speedup (cold and hot)
        ax2 = ax.twinx()
        ax2.grid(False)
        df_speed_q = speedup_df[(speedup_df["Query"] == qid_num) & (speedup_df["Op"] == op)].sort_values("ConfigIdx")
        positions_s = df_speed_q["ConfigIdx"].tolist()
        # 1x reference line: speedup is positive (>1) above this line
        ax2.axhline(1, color="gray", linestyle=":", linewidth=1.2, alpha=0.85, zorder=1)
        line_cold, = ax2.plot(
            positions_s, df_speed_q["SpeedupCold"],
            color="green", linestyle="-", marker="s",
            label="Speedup (cold runs)", markerfacecolor="green",
        )
        line_hot, = ax2.plot(
            positions_s, df_speed_q["SpeedupHot"],
            color="#0b6623", linestyle="--", marker="d",
            label="Speedup (hot runs)", markerfacecolor="#0b6623",
        )
        y2_top = np.nanmax([df_speed_q["SpeedupCold"].max(), df_speed_q["SpeedupHot"].max()])
        if not np.isfinite(y2_top) or y2_top <= 0:
            y2_top = 1.0
        ax2.set_ylim(0, y2_top * 1.15)
        ax2.set_ylabel(r"Speedup ($\times$)", color="green")
        ax2.tick_params(axis='y', labelcolor='green')
        ticks = speedup_ticks(y2_top * 1.15)
        drop = set(TICK_DROPS.get((qid_num, op), []))
        ax2.set_yticks([t for t in ticks if t not in drop])
        ax2.yaxis.set_major_formatter(no_zero_formatter)
        if i == 0:
            legend_handles.extend([line_cold, line_hot])

    # Sort legend: Baseline first, then our approach, then speedup series
    sorted_handles = sorted(
        legend_handles,
        key=lambda h: (
            not h.get_label().startswith(BASELINE_DISPLAY),
            not h.get_label().startswith(ARRAY_DISPLAY),
        ),
    )

    axes[3].legend(
        handles=sorted_handles,
        title="Scenario / Speedup",
        loc="upper right",
        bbox_to_anchor=(1.1, -0.35),
        ncol=3,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.subplots_adjust(wspace=0.4)

    out_pdf = os.path.join(os.path.dirname(__file__), f"{output_basename}.pdf")
    out_png = os.path.join(os.path.dirname(__file__), f"{output_basename}.png")
    plt.savefig(out_png, dpi=200, bbox_inches="tight")
    plt.savefig(out_pdf, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_pdf}")
    print(f"Saved {out_png}")


make_plot("COUNT", "duckdb_plot-general-scaling-speedup-count")
make_plot("COPY", "duckdb_plot-general-scaling-speedup-copy")
