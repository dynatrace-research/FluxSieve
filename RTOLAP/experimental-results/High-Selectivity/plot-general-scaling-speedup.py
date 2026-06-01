# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
import glob
import re
import os

# ==========================
# Configuration
# ==========================
FOLDERS = ["5M-records", "10M-records", "20M-records", "40M-records"]

# Two patterns: originals and count-query files
FILE_PATTERN_ORIG = "*-logs-all-run*.csv"
FILE_PATTERN_COUNT = "*-logs-all-run*-count-queries.csv"

SCENARIOS = ["streaming", "text_indexed"]

# Original queries and count queries
ORIG_QUERIES = ["Q1", "Q2", "Q3", "Q4"]
COUNT_QUERIES = ["Q1-count", "Q2-count", "Q4-count"]
ALL_QUERIES_ORDERED = ORIG_QUERIES + COUNT_QUERIES

# Group labels (titles) - matching first script style
QUERY_LABELS = {
    "Q1": "Q1 - Single cond. filter\n(ultra rare matches)",
    "Q2": "Q2 - Single cond. filter\n(rare matches)",
    "Q3": "Q3 - Single cond. filter\nand aggregation",
    "Q4": "Q4 - Multi column filter\n(two cond.)",
    "Q1_Count": "Q1 with count\nfilter + aggregation",
    "Q2_Count": "Q2 with count\nfilter + aggregation",
    "Q4_Count": "Q4 with count\n2 filters + aggregation",
}

N_BOOT = 10000
ALPHA = 0.05
RNG = np.random.default_rng(42)

# ==========================
# Style Configuration (from first script)
# ==========================
plt.rcParams['figure.figsize'] = [3.33, 2.22]
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['font.size'] = 23
plt.rcParams['font.family'] = 'serif'
plt.rcParams['axes.grid'] = True
plt.rcParams["axes.grid.axis"] = "y"
plt.rcParams['grid.color'] = 'dddddd'
plt.rcParams['axes.axisbelow'] = True
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.preamble'] = r"""
        \usepackage{libertine}
        \usepackage[libertine]{newtxmath}
        """

# ==========================
# Helpers
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

def parse_scenario(comment):
    s = str(comment)
    if re.search(r'-streaming\b', s):
        return "streaming"
    if re.search(r'-text_indexed\b', s):
        return "text_indexed"
    return None

def extract_query_tokens(comment):
    s = str(comment)
    # Match count queries first
    m_count = re.match(r'^(Q\d+)([a-d]?)-count\b', s)
    if m_count:
        base = f"{m_count.group(1)}-count"
        suffix = m_count.group(2)
        token = f"{m_count.group(1)}{suffix}-count" if suffix else base
        return token, base

    # Original queries
    m_orig = re.match(r'^(Q\d+)([a-d]?)\b', s)
    if m_orig:
        base = m_orig.group(1)
        suffix = m_orig.group(2)
        token = base if suffix == "" else f"{base}{suffix}"
        return token, base

    return None, None

def token_to_normalized_query(base):
    return base

def base_to_query_num(base):
    m = re.match(r'^Q(\d+)', base.replace('-count', ''))
    return int(m.group(1)) if m else None

def is_count_base(base):
    return isinstance(base, str) and base.endswith('-count')

def title_key_from_normquery(norm_query):
    return norm_query.replace("-count", "_Count")

# ==========================
# Collect stats per query
# ==========================
records_stats = []

for folder in FOLDERS:
    files_orig = glob.glob(os.path.join(folder, FILE_PATTERN_ORIG))
    files_count = glob.glob(os.path.join(folder, FILE_PATTERN_COUNT))

    files_orig = [f for f in files_orig if "count" not in os.path.basename(f)]
    files_count = [f for f in files_count if "count" in os.path.basename(f)]

    files = files_orig + files_count
    if not files:
        raise FileNotFoundError(f"No files found in {folder} matching {FILE_PATTERN_ORIG} or {FILE_PATTERN_COUNT}")

    dfs = []
    for f in files:
        df_f = pd.read_csv(f)
        required = {"Comment", "Query", "TimeUsedMs"}
        if not required.issubset(df_f.columns):
            raise KeyError(f"Missing columns in {f}")
        df_f["TimeUsedMs"] = pd.to_numeric(df_f["TimeUsedMs"], errors="coerce")
        df_f["Scenario"] = df_f["Comment"].apply(parse_scenario)

        tokens = df_f["Comment"].apply(extract_query_tokens)
        df_f["QueryToken"] = tokens.apply(lambda t: t[0])
        df_f["BaseQ"] = tokens.apply(lambda t: t[1])

        df_f["NormQuery"] = df_f["BaseQ"].apply(token_to_normalized_query)
        df_f["QueryNum"] = df_f["BaseQ"].apply(base_to_query_num)
        df_f["IsCount"] = df_f["BaseQ"].apply(is_count_base)
        df_f["__source_file"] = f
        dfs.append(df_f)

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.dropna(subset=["Scenario", "QueryToken", "BaseQ", "NormQuery"])
    df_all = df_all[df_all["NormQuery"].isin(ALL_QUERIES_ORDERED)]

    # Cold runs
    cold_rows = []
    for f in files:
        df_f = df_all[df_all["__source_file"] == f]
        df_cold = df_f[df_f["QueryToken"] == df_f["BaseQ"]].copy()
        df_cold_first = df_cold.drop_duplicates(subset=["BaseQ", "Scenario"], keep="first")
        cold_rows.append(df_cold_first[["NormQuery", "BaseQ", "Scenario", "TimeUsedMs"]])
    cold_all = pd.concat(cold_rows, ignore_index=True) if cold_rows else pd.DataFrame(columns=["NormQuery","BaseQ","Scenario","TimeUsedMs"])

    # Hot runs
    def has_letter_suffix(token, base):
        if base is None or token is None:
            return False
        b = base.replace("-count", "")
        t = token.replace("-count", "")
        return (t != b) and re.match(r'^Q\d+[a-d]', t) is not None

    df_all["IsHotToken"] = df_all.apply(lambda r: has_letter_suffix(r["QueryToken"], r["BaseQ"]), axis=1)
    hot_df = df_all[df_all["IsHotToken"]].copy()

    # Aggregate medians
    for qid in ALL_QUERIES_ORDERED:
        for scenario in SCENARIOS:
            # Cold
            sub_cold = cold_all[(cold_all["Scenario"] == scenario) & (cold_all["NormQuery"] == qid)]["TimeUsedMs"]
            med_cold, lo_cold, hi_cold = bootstrap_percentile_ci_median(sub_cold, alpha=ALPHA, n_boot=N_BOOT, rng=RNG)
            records_stats.append({
                "Records": int(folder.replace("M-records", "")),
                "QueryLabel": qid,
                "Scenario": scenario,
                "Type": "cold runs",
                "Median": med_cold, "CI_low": lo_cold, "CI_high": hi_cold
            })

            # Hot
            sub_hot = hot_df[(hot_df["Scenario"] == scenario) & (hot_df["NormQuery"] == qid)]["TimeUsedMs"]
            med_hot, lo_hot, hi_hot = bootstrap_percentile_ci_median(sub_hot, alpha=ALPHA, n_boot=N_BOOT, rng=RNG)
            records_stats.append({
                "Records": int(folder.replace("M-records", "")),
                "QueryLabel": qid,
                "Scenario": scenario,
                "Type": "hot runs",
                "Median": med_hot, "CI_low": lo_hot, "CI_high": hi_hot
            })

# ==========================
# Prepare DataFrame
# ==========================
stats_df = pd.DataFrame(records_stats)
if stats_df.empty:
    raise RuntimeError("No stats collected. Check file patterns or parsing logic.")

stats_df = stats_df.sort_values(["QueryLabel", "Records"])

# Compute speedup
speedup_rows = []
for qid in ALL_QUERIES_ORDERED:
    df_q = stats_df[stats_df["QueryLabel"] == qid]
    for r in sorted(df_q["Records"].unique()):
        cold_stream = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "streaming") & (df_q["Type"] == "cold runs")]["Median"].values
        cold_text   = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "text_indexed") & (df_q["Type"] == "cold runs")]["Median"].values

        hot_stream = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "streaming") & (df_q["Type"] == "hot runs")]["Median"].values
        hot_text   = df_q[(df_q["Records"] == r) & (df_q["Scenario"] == "text_indexed") & (df_q["Type"] == "hot runs")]["Median"].values

        cold_su = np.nan
        hot_su = np.nan
        if cold_stream.size and cold_text.size and np.isfinite(cold_stream[0]) and np.isfinite(cold_text[0]) and cold_stream[0] != 0:
            cold_su = cold_text[0] / cold_stream[0]
        if hot_stream.size and hot_text.size and np.isfinite(hot_stream[0]) and np.isfinite(hot_text[0]) and hot_stream[0] != 0:
            hot_su = hot_text[0] / hot_stream[0]

        speedup_rows.append({"QueryLabel": qid, "Records": r, "SpeedupCold": cold_su, "SpeedupHot": hot_su})

speedup_df = pd.DataFrame(speedup_rows)


# Per-query tick values to drop, for high-range subplots where the
# auto-picker keeps low-range ticks (5, 15, 25) that crowd the bottom.
# Keys match `qlabel` strings in ALL_QUERIES_ORDERED.
TICK_DROPS = {
    "Q4": [5, 15, 25],
    "Q4-count": [5, 15, 25],
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
# Plot: 2x4 grid with fixed layout
# ==========================
fig, axes = plt.subplots(2, 4, figsize=(20, 10), sharex=True)
axes = axes.flatten()

n_needed = len(ALL_QUERIES_ORDERED)
for j in range(n_needed, len(axes)):
    axes[j].set_visible(False)
    axes[j].axis('off')

usable_axes = axes[:n_needed]

# Colors and linestyles matching first script
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

for i, qlabel in enumerate(ALL_QUERIES_ORDERED):
    ax = usable_axes[i]
    df_q = stats_df[stats_df["QueryLabel"] == qlabel].sort_values("Records")

    # Plot execution time with labels matching first script
    for (scenario, t), grp in df_q.groupby(["Scenario", "Type"]):
        grp = grp.sort_values("Records")
        positions = [x_positions[FOLDERS.index(f"{r}M-records")] for r in grp["Records"]]
        scenario_label = "Baseline: FTS index" if scenario == "text_indexed" else "Our approach: FluxSieve"
        label = f"{scenario_label} ({t})"
        line = ax.errorbar(
            positions, grp["Median"],
            yerr=[grp["Median"] - grp["CI_low"], grp["CI_high"] - grp["Median"]],
            label=label,
            color=colors[(scenario, t)],
            linestyle=linestyles[t],
            marker="o", capsize=4
        )
        if i == 0:
            legend_handles.append(line)

    # Title
    title_key = title_key_from_normquery(qlabel)
    title = QUERY_LABELS.get(title_key, qlabel)
    ax.set_title(title)

    # Grid
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels)
    
    # Y-axis label on ALL plots
    ax.set_ylabel(r"Median time (ms) [95\% CI]")
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, integer=True))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(y)}' if y == int(y) else f'{y:g}'))
    
    # X-axis label only on bottom row
    if i >= 4:
        ax.set_xlabel("Records")

    # Secondary axis for speedup
    ax2 = ax.twinx()
    ax2.grid(False)
    df_speed_q = speedup_df[speedup_df["QueryLabel"] == qlabel].sort_values("Records")

    positions_speed = [x_positions[FOLDERS.index(f"{r}M-records")] for r in df_speed_q["Records"]]

    # 1x reference line: speedup is positive (>1) above this line
    ax2.axhline(1, color="gray", linestyle=":", linewidth=1.2, alpha=0.85, zorder=1)

    # Speedup Cold
    line_cold, = ax2.plot(
        positions_speed,
        df_speed_q["SpeedupCold"],
        color="green",
        linestyle="-",
        marker="s",
        label="Speedup (cold runs)",
        markerfacecolor="green"
    )
    # Speedup Hot
    line_hot, = ax2.plot(
        positions_speed,
        df_speed_q["SpeedupHot"],
        color="#0b6623",
        linestyle="--",
        marker="d",
        label="Speedup (hot runs)",
        markerfacecolor="#0b6623"
    )

    y2_top = np.nanmax([df_speed_q["SpeedupCold"].max(), df_speed_q["SpeedupHot"].max()])
    if not np.isfinite(y2_top) or y2_top <= 0:
        y2_top = 1.0
    ax2.set_ylim(0, y2_top * 1.15)
    ax2.set_ylabel(r"Speedup ($\times$)", color="green", labelpad=.1)
    ax2.tick_params(axis='y', labelcolor='green', pad=4)
    ticks = speedup_ticks(y2_top * 1.15)
    drop = set(TICK_DROPS.get(qlabel, []))
    ax2.set_yticks([t for t in ticks if t not in drop])
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{int(round(y)):>3d}'))

    if i == 0:
        legend_handles.extend([line_cold, line_hot])

# Sort legend: matching first script order
sorted_handles = sorted(
    legend_handles,
    key=lambda h: h.get_label().startswith("Baseline"),
    reverse=True
)

# Legend placement - centered below all plots
# fig.legend(
#     handles=sorted_handles,
#     title="Scenario / Speedup",
#     loc="lower center",
#     bbox_to_anchor=(0.864, .18),
#     ncol=1,
#     fontsize=19,
#     title_fontsize=21
# )

axes[6].legend(
    handles=sorted_handles,
    title="Scenario / Speedup",
    loc="upper right",
    bbox_to_anchor=(2.4, -0.2),  # shift below the axes
    ncol=3,                       # <-- choose number of columns
)

plt.tight_layout(rect=[0, 0.05, 1, 1])
plt.subplots_adjust(hspace=0.35, wspace=0.68)  # increase horizontal spacing between subplots

plt.savefig("hsq_plot-general-scaling-speedup.png", dpi=200, bbox_inches="tight")
plt.savefig("hsq_plot-general-scaling-speedup.pdf", dpi=200, bbox_inches="tight")
print("Plots saved successfully!")
