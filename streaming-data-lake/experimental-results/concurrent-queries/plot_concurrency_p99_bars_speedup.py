import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


BASELINE_LABEL = "BASELINE"
FLUX_LABEL = "FLUXSIEVE"


def find_latest_summary(results_root: Path, scenario_prefix: str) -> Path:
    candidates = sorted(
        [path for path in results_root.glob(f"{scenario_prefix}_*") if path.is_dir()]
    )
    if not candidates:
        raise FileNotFoundError(
            f"No result folders found in {results_root} with prefix '{scenario_prefix}_*'"
        )

    summary_csv = candidates[-1] / "summary_overall.csv"
    if not summary_csv.exists():
        raise FileNotFoundError(f"Missing summary file: {summary_csv}")
    return summary_csv


def load_and_prepare(summary_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(summary_csv)
    required = {"concurrency_level", "system", "median_latency_ms", "p99_latency_ms", "qps"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in {summary_csv}: {sorted(missing)}")

    df = df.copy()
    df["concurrency_level"] = pd.to_numeric(df["concurrency_level"], errors="coerce")
    df["median_latency_ms"] = pd.to_numeric(df["median_latency_ms"], errors="coerce")
    df["p99_latency_ms"] = pd.to_numeric(df["p99_latency_ms"], errors="coerce")
    df["qps"] = pd.to_numeric(df["qps"], errors="coerce")
    df = df.dropna(subset=["concurrency_level", "system", "median_latency_ms", "p99_latency_ms", "qps"])
    df = df.sort_values(["concurrency_level", "system"]).reset_index(drop=True)
    return df


def format_ms(value: float) -> str:
    return f"{value:.1f}"


def annotate_bars(ax: plt.Axes, bars, values: np.ndarray) -> None:
    ymax = max(values) if len(values) else 0.0
    offset = max(ymax * 0.015, 1.0)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + offset,
            format_ms(value),
            ha="center",
            va="bottom",
            fontsize=11,
            rotation=0,
        )


def plot_grouped_bars_with_speedup(df: pd.DataFrame, out_prefix: Path, title: str) -> None:
    plt.rcParams["figure.figsize"] = [6.8, 3.8]
    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42
    plt.rcParams["font.size"] = 16
    plt.rcParams["font.family"] = "serif"
    plt.rcParams["axes.grid"] = True
    plt.rcParams["axes.grid.axis"] = "y"
    plt.rcParams["grid.color"] = "#dddddd"
    plt.rcParams["axes.axisbelow"] = True

    colors = {
        BASELINE_LABEL: "#ff7f0e",
        FLUX_LABEL: "#1f77b4",
    }

    p99_pivot = df.pivot(index="concurrency_level", columns="system", values="p99_latency_ms").sort_index()
    median_pivot = df.pivot(index="concurrency_level", columns="system", values="median_latency_ms").sort_index()

    for needed in [BASELINE_LABEL, FLUX_LABEL]:
        if needed not in p99_pivot.columns or needed not in median_pivot.columns:
            raise ValueError(f"Missing system '{needed}' in summary data")

    levels = p99_pivot.index.astype(int).tolist()
    x = np.arange(len(levels), dtype=float)
    width = 0.18

    baseline_median = median_pivot[BASELINE_LABEL].to_numpy(dtype=float) / 1000.0
    baseline_p99 = p99_pivot[BASELINE_LABEL].to_numpy(dtype=float) / 1000.0
    flux_median = median_pivot[FLUX_LABEL].to_numpy(dtype=float) / 1000.0
    flux_p99 = p99_pivot[FLUX_LABEL].to_numpy(dtype=float) / 1000.0
    qps_pivot = df.pivot(index="concurrency_level", columns="system", values="qps").sort_index()
    baseline_qps = qps_pivot[BASELINE_LABEL].to_numpy(dtype=float)
    flux_qps = qps_pivot[FLUX_LABEL].to_numpy(dtype=float)
    speedup = flux_qps / baseline_qps

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax2.grid(False)

    bars_baseline_median = ax1.bar(
        x - 1.5 * width,
        baseline_median,
        width,
        color=colors[BASELINE_LABEL],
        hatch="",
        label="Baseline median",
    )
    bars_baseline_p99 = ax1.bar(
        x - 0.5 * width,
        baseline_p99,
        width,
        color=colors[BASELINE_LABEL],
        hatch="//",
        label="Baseline P99",
    )
    bars_flux_median = ax1.bar(
        x + 0.5 * width,
        flux_median,
        width,
        color=colors[FLUX_LABEL],
        hatch="",
        label="FluxSieve median",
    )
    bars_flux_p99 = ax1.bar(
        x + 1.5 * width,
        flux_p99,
        width,
        color=colors[FLUX_LABEL],
        hatch="//",
        label="FluxSieve P99",
    )

    ax2.plot(
        x,
        speedup,
        color="green",
        marker="o",
        linewidth=2,
        linestyle="-",
        label="FluxSieve QPS speedup",
    )

    annotate_bars(ax1, bars_baseline_median, baseline_median)
    annotate_bars(ax1, bars_baseline_p99, baseline_p99)
    annotate_bars(ax1, bars_flux_median, flux_median)
    annotate_bars(ax1, bars_flux_p99, flux_p99)

    ax1.set_xticks(x)
    ax1.set_xticklabels([str(level) for level in levels])
    ax1.set_xlabel("Concurrency (clients)")
    ax1.set_ylabel("Latency (s)")
    ax2.set_ylabel("FluxSieve QPS speedup (x)", color="green")
    ax2.tick_params(axis="y", colors="green")
    if title:
        ax1.set_title(title)

    ymax = max(np.max(baseline_median), np.max(baseline_p99), np.max(flux_median), np.max(flux_p99))
    ax1.set_ylim(0, ymax * 2.2)
    ax2.set_ylim(0, np.max(speedup) * 1.6)

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(
        h1 + h2,
        l1 + l2,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.98),
        ncol=2,
        frameon=False,
        fontsize=13.5,
    )

    fig.subplots_adjust(left=0.10, right=0.90, top=0.96, bottom=0.14)
    png_path = out_prefix.with_suffix(".png")
    pdf_path = out_prefix.with_suffix(".pdf")
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved: {png_path}")
    print(f"Saved: {pdf_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot grouped absolute median/P99 bars plus FluxSieve P99 speedup line from concurrency summary_overall.csv"
    )
    parser.add_argument(
        "--summary-csv",
        default="",
        help="Path to summary_overall.csv. If omitted, auto-detect latest run by prefix.",
    )
    parser.add_argument(
        "--results-root",
        default="concurrency-results",
        help="Root folder containing timestamped run directories.",
    )
    parser.add_argument(
        "--scenario-prefix",
        default="10M-concurrency-fixed",
        help="Run folder prefix used when auto-detecting latest run.",
    )
    parser.add_argument(
        "--out-prefix",
        default="",
        help="Output path prefix without extension. Defaults to <run-folder>/latency_bars_speedup.",
    )
    parser.add_argument(
        "--title",
        default="",
        help="Figure title.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.summary_csv:
        summary_csv = Path(args.summary_csv).resolve()
        if not summary_csv.exists():
            raise FileNotFoundError(f"Summary CSV not found: {summary_csv}")
    else:
        summary_csv = find_latest_summary(Path(args.results_root), args.scenario_prefix).resolve()

    run_dir = summary_csv.parent
    out_prefix = Path(args.out_prefix).resolve() if args.out_prefix else (run_dir / "latency_bars_speedup")

    df = load_and_prepare(summary_csv)
    plot_grouped_bars_with_speedup(df, out_prefix, args.title)


if __name__ == "__main__":
    main()
