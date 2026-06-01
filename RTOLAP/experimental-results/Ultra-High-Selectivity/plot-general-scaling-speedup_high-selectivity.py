# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter, FuncFormatter
import glob
import re
import os

# ==========================
# Configuration
# ==========================
FOLDERS = ["5M-records", "10M-records", "20M-records", "40M-records"]
FILE_PATTERN = "*-logs-all-run*.csv"
SCENARIOS = ["streaming", "text_indexed"]
QUERIES_OF_INTEREST = [1, 2, 3, 4]
N_BOOT = 10000
ALPHA = 0.05
RNG = np.random.default_rng(42)

# Query titles
QUERY_LABELS = {
    "Q1": "Q1 - Single cond. filter (no matches)",
    "Q2": "Q2 - Single cond. filter (ultra rare matches)",
    "Q3": "Q3 - Single cond. filter and aggregation",
    "Q4": "Q4 - Multi column filter (two cond.)"
}

plt.rcParams['figure.figsize'] = [3.33, 2.22]
plt.rcParams['pdf.fonttype'] = 42 # TrueType fonts
plt.rcParams['ps.fonttype'] = 42 # TrueType fonts
plt.rcParams['font.size'] = 19 # ACM LaTeX template default
plt.rcParams['font.family'] = 'serif' # Use text font
plt.rcParams['axes.grid'] = True
plt.rcParams["axes.grid.axis"] = "y"
plt.rcParams['grid.color']='dddddd'
plt.rcParams['axes.axisbelow'] = True
#plt.rcParams['lines.linewidth'] = 0.5
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r"""
        \usepackage{libertine}
        \usepackage[libertine]{newtxmath}
        """

# ==========================
# Bootstrap CI for median
# ==========================
def bootstrap_percentile_ci_median(values, alpha=0.05, n_boot=10000, rng=None):
    vals = np.asarray(values)
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

# ==========================
# Parse scenario from comment
# ==========================
def parse_scenario(comment):
    s = str(comment)
    if re.search(r'-streaming\b', s):
        return "streaming"
    if re.search(r'-text_indexed\b', s):
        return "text_indexed"
    return None

# ==========================
# Collect stats per query
# ==========================
records_stats = []

for folder in FOLDERS:
    files = glob.glob(os.path.join(folder, FILE_PATTERN))
    if not files:
        raise FileNotFoundError(f"No files found in {folder} matching {FILE_PATTERN}")
    
    dfs = []
    for f in files:
        df_f = pd.read_csv(f)
        required = {"Comment", "Query", "TimeUsedMs"}
        if not required.issubset(df_f.columns):
            raise KeyError(f"Missing columns in {f}")
        df_f["TimeUsedMs"] = pd.to_numeric(df_f["TimeUsedMs"], errors="coerce")
        df_f["Scenario"] = df_f["Comment"].apply(parse_scenario)
        df_f["QueryToken"] = df_f["Comment"].str.extract(r'^(Q\d+[a-d]?)\b')
        df_f["BaseQ"] = df_f["QueryToken"].str.extract(r'^(Q\d+)')
        df_f["QueryNum"] = df_f["BaseQ"].str.extract(r'Q(\d+)').astype(float)
        df_f["__source_file"] = f
        dfs.append(df_f)
    
    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.dropna(subset=["Scenario", "QueryToken", "BaseQ", "QueryNum"])
    df_all = df_all[df_all["QueryNum"].isin(QUERIES_OF_INTEREST)]
    
    # Cold runs: first occurrence per query/scenario
    cold_rows = []
    for f in files:
        df_f = df_all[df_all["__source_file"] == f]
        df_cold = df_f[df_f["QueryToken"] == df_f["BaseQ"]].copy()
        df_cold_first = df_cold.drop_duplicates(subset=["BaseQ", "Scenario"], keep="first")
        cold_rows.append(df_cold_first[["BaseQ", "Scenario", "TimeUsedMs"]])
    cold_all = pd.concat(cold_rows, ignore_index=True)
    
    # Hot runs: tokens with letter suffix
    hot_df = df_all[df_all["QueryToken"].str.match(r'^Q\d+[a-d]', na=False)].copy()
    
    # Aggregate medians per query/scenario/type
    for qid in QUERIES_OF_INTEREST:
        for scenario in SCENARIOS:
            # Cold
            sub_cold = cold_all[(cold_all["Scenario"] == scenario) & (cold_all["BaseQ"] == f"Q{qid}")]["TimeUsedMs"]
            med_cold, lo_cold, hi_cold = bootstrap_percentile_ci_median(sub_cold, alpha=ALPHA, n_boot=N_BOOT, rng=RNG)
            records_stats.append({"Records": folder.replace("M-records", ""), "Query": qid, "Scenario": scenario, "Type": "cold runs", "Median": med_cold, "CI_low": lo_cold, "CI_high": hi_cold})
            
            # Hot
            sub_hot = hot_df[(hot_df["Scenario"] == scenario) & (hot_df["BaseQ"] == f"Q{qid}")]["TimeUsedMs"]
            med_hot, lo_hot, hi_hot = bootstrap_percentile_ci_median(sub_hot, alpha=ALPHA, n_boot=N_BOOT, rng=RNG)
            records_stats.append({"Records": folder.replace("M-records", ""), "Query": qid, "Scenario": scenario, "Type": "hot runs", "Median": med_hot, "CI_low": lo_hot, "CI_high": hi_hot})

# ==========================
# Prepare DataFrame
# ==========================
stats_df = pd.DataFrame(records_stats)
stats_df["Records"] = stats_df["Records"].astype(int)
stats_df = stats_df.sort_values(["Query", "Records"])

# ==========================
# Compute speedups per query and records (Cold and Hot separately)
# ==========================
speedup_rows = []
for qid in QUERIES_OF_INTEREST:
    df_q = stats_df[stats_df["Query"] == qid]
    for r in sorted(df_q["Records"].unique()):
        # Cold median per scenario
        cold_stream = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "streaming") & (df_q["Type"] == "cold runs")]["Median"].values
        cold_text   = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "text_indexed") & (df_q["Type"] == "cold runs")]["Median"].values
        # Hot median per scenario
        hot_stream = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "streaming") & (df_q["Type"] == "hot runs")]["Median"].values
        hot_text   = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "text_indexed") & (df_q["Type"] == "hot runs")]["Median"].values

        # Compute speedups robustly
        su_cold = np.nan
        su_hot = np.nan
        if cold_stream.size and cold_text.size and np.isfinite(cold_stream[0]) and np.isfinite(cold_text[0]) and cold_stream[0] != 0:
            su_cold = cold_text[0] / cold_stream[0]
        if hot_stream.size and hot_text.size and np.isfinite(hot_stream[0]) and np.isfinite(hot_text[0]) and hot_stream[0] != 0:
            su_hot = hot_text[0] / hot_stream[0]

        speedup_rows.append({"Query": qid, "Records": r, "SpeedupCold": su_cold, "SpeedupHot": su_hot})

speedup_df = pd.DataFrame(speedup_rows)

# ==========================
# Custom formatter: hide 0 tick label
# ==========================
def fmt_no_zero(x, pos):
    return f'{x:g}'

no_zero_formatter = FuncFormatter(fmt_no_zero)


# Per-query explicit y2 tick overrides. When set, the auto-picker is
# bypassed and these exact values are used. Keyed by query number (int).
TICK_OVERRIDES = {
    2: [1, 2, 3, 4, 5, 6],
    3: [1, 2, 3, 4, 5, 6, 7, 8],
}

# Per-query tick values to drop from the auto-picked set.
TICK_DROPS = {
    1: [2, 5, 15, 25],
    4: [2, 5, 15, 25],
}


def speedup_ticks(y2_top, min_gap_frac=0.035):
    """Adaptive speedup-axis ticks. Walks a dense ladder of round numbers and
    keeps each candidate only if it is at least `min_gap_frac` of the axis
    range away from the previously kept tick. Always includes 1
    (speedup=1 reference)."""
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
# Plot: Multi-subplot (one per query)
# ==========================
fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
axes = axes.flatten()

colors = {
    ("streaming", "cold runs"): "#1f77b4",
    ("streaming", "hot runs"): "#1f77b4",
    ("text_indexed", "cold runs"): "#ff7f0e",
    ("text_indexed", "hot runs"): "#ff7f0e"
}
linestyles = {"cold runs": "-", "hot runs": "--"}

x_positions = [0, 1, 2, 3]
x_labels = ["5M", "10M", "20M", "40M"]

legend_handles = []

for i, qid in enumerate(QUERIES_OF_INTEREST):
    ax = axes[i]
    df_q = stats_df[stats_df["Query"] == qid]

    # Plot execution time with custom labels
    for (scenario, t), grp in df_q.groupby(["Scenario", "Type"]):
        grp = grp.sort_values("Records")
        positions = [x_positions[FOLDERS.index(f"{r}M-records")] for r in grp["Records"]]
        scenario_label = "Baseline: FTS index" if scenario == "text_indexed" else "Our approach: FluxSieve"
        label = f"{scenario_label} ({t})"
        line = ax.errorbar(
            positions, grp["Median"], 
            yerr=[grp["Median"] - grp["CI_low"], grp["CI_high"] - grp["Median"]],
            label=label, color=colors[(scenario, t)], linestyle=linestyles[t], marker="o", capsize=4
        )
        if i == 0:
            legend_handles.append(line)

    # Title from QUERY_LABELS
    ax.set_title(QUERY_LABELS.get(f"Q{qid}", f"Query {qid}"))

    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels)
    ax.set_ylabel(r"Median time (ms) [95\% CI]")
    if i >= 2:
        ax.set_xlabel("Records")
    ax.yaxis.set_major_formatter(no_zero_formatter)

    # Secondary axis for speedup (Cold and Hot)
    ax2 = ax.twinx()
    ax2.grid(False)
    df_speed_q = speedup_df[speedup_df["Query"] == qid].sort_values("Records")

    positions_speed = [x_positions[FOLDERS.index(f"{r}M-records")] for r in df_speed_q["Records"]]

    # 1x reference line: speedup is positive (>1) above this line
    ax2.axhline(1, color="gray", linestyle=":", linewidth=1.2, alpha=0.85, zorder=1)

    # Speedup Cold
    line_cold, = ax2.plot(
        positions_speed, df_speed_q["SpeedupCold"],
        color="green", linestyle="-", marker="s", label="Speedup (cold runs)", markerfacecolor="green"
    )
    # Speedup Hot
    line_hot, = ax2.plot(
        positions_speed, df_speed_q["SpeedupHot"],
        color="#0b6623", linestyle="--", marker="d", label="Speedup (hot runs)", markerfacecolor="#0b6623"
    )

    # y2 axis starts at 0 with more headroom
    y2_top = np.nanmax([df_speed_q["SpeedupCold"].max(), df_speed_q["SpeedupHot"].max()])
    if not np.isfinite(y2_top) or y2_top <= 0:
        y2_top = 1.0
    if qid in TICK_OVERRIDES:
        override = TICK_OVERRIDES[qid]
        ax2.set_ylim(0, max(override) * 1.15)
        ticks = override
    else:
        ax2.set_ylim(0, y2_top * 1.15)
        drop = set(TICK_DROPS.get(qid, []))
        ticks = [t for t in speedup_ticks(y2_top * 1.15) if t not in drop]
    ax2.set_ylabel(r"Speedup ($\times$)", color="green")
    ax2.tick_params(axis='y', labelcolor='green')
    ax2.set_yticks(ticks)
    ax2.yaxis.set_major_formatter(no_zero_formatter)
    if i == 0:
        legend_handles.extend([line_cold, line_hot])

# Sort legend: Baseline first, then our approach, then speedup series
sorted_handles = sorted(
    legend_handles,
    key=lambda h: (
        not h.get_label().startswith("Baseline: FTS index"),
        not h.get_label().startswith("Our approach: FluxSieve"),
    )
)

axes[3].legend(
    handles=sorted_handles,
    title="Scenario / Speedup",
    loc="upper right",
    bbox_to_anchor=(1.1, -0.2),  # shift below the axes
    ncol=3,                       # <-- choose number of columns
)
#fig.suptitle("Query Execution Time vs Dataset Size", fontsize=14)
plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.subplots_adjust(wspace=0.3)  # Reduce horizontal space between columns

plt.savefig("uhsq_plot-general-scaling-speedup.png", dpi=200, bbox_inches="tight")
plt.savefig("uhsq_plot-general-scaling-speedup.pdf", dpi=200, bbox_inches="tight")

