import argparse
import pandas as pd
import os
import matplotlib.pyplot as plt

from learning.dataset.dataset_creation import extract_labels_from_directory

plt.style.use('seaborn-v0_8')
order = ['Linear', 'Linear\nwith Agg.', '2-Way-Join', '2-Way-J.\nwith Agg.', '3-Way-Join', '3-Way-J.\nwith Agg.']


def plot(costream_results, baseline_results, metric):
    assert os.path.exists(costream_results) and costream_results.endswith(".csv")
    assert os.path.exists(baseline_results) and baseline_results.endswith(".csv")

    # Read prediction results
    costream_df = pd.read_csv(costream_results)
    costream_df = costream_df.set_index("query")

    # Read optimized query results
    labels = extract_labels_from_directory(queries_path=args.costream_queries,
                                           exclude_failing_queries=False,
                                           offset_to_boolean=True,
                                           stratify_offsets=False,
                                           stratify_failing=False,
                                           filters=None)

    labels.index = labels.index.str.replace('-optimized', '')
    costream_df = pd.merge(costream_df, labels[[metric, "offset"]], left_index=True, right_index=True)
    costream_df["real_speedup"] = costream_df["initial_true_value"] / costream_df[metric]
    costream_df = costream_df[costream_df[metric] != -1]
    print(len(costream_df[costream_df["query_type"] == "Linear\nQuery"]))

    baseline_df = pd.read_csv(baseline_results)
    baseline_df = baseline_df.set_index("query")

    # Read optimized query results
    labels = extract_labels_from_directory(queries_path=args.baseline_queries,
                                           exclude_failing_queries=False,
                                           offset_to_boolean=True,
                                           stratify_offsets=False,
                                           stratify_failing=False,
                                           filters=None)

    labels.index = labels.index.str.replace('-optimized', '')
    baseline_df = pd.merge(baseline_df, labels[[metric, "offset"]], left_index=True, right_index=True)
    baseline_df["real_speedup"] = baseline_df["initial_true_value"] / baseline_df[metric]
    baseline_df = baseline_df[baseline_df[metric] != -1]

    costream_speed_ups = costream_df.groupby("query_type").agg('median')["real_speedup"]
    baseline_speed_ups = baseline_df.groupby("query_type").agg('median')["real_speedup"]
    df = pd.merge(costream_speed_ups, baseline_speed_ups,
                  left_index=True,
                  right_index=True,
                  suffixes=["_costream", "_baseline"])

    df = df.rename(columns={"real_speedup_costream": "COSTREAM",
                            "real_speedup_baseline": "Flat Vector"})

    df['query_type'] = df.index
    df['query_type'] = df['query_type'].str.replace('Linear\nQuery', 'Linear')
    df['query_type'] = df['query_type'].str.replace('Linear\nwith Aggregation', 'Linear\nwith Agg.')
    df['query_type'] = df['query_type'].str.replace('2-Way-Join\nQuery', '2-Way-Join')
    df['query_type'] = df['query_type'].str.replace('2-Way-Join\nwith Aggregation', '2-Way-J.\nwith Agg.')
    df['query_type'] = df['query_type'].str.replace('3-Way-Join\nQuery', '3-Way-Join')
    df['query_type'] = df['query_type'].str.replace('3-Way-Join\nwith Aggregation', '3-Way-J.\nwith Agg.')

    # Order bars
    df = df.iloc[df["query_type"].map({o: i for i, o in enumerate(order)}).argsort()]

    # Plot bars
    fig, ax = plt.subplots(1, 1, figsize=(8, 2.5))
    df.plot.bar(ax=ax, ecolor='black', capsize=10, width=0.8, edgecolor="black")
    ax.tick_params(axis='x', colors='black', rotation=0, labelsize=15)
    ax.tick_params(axis='y', colors='black', labelsize=15)
    ax.set_ylabel("Median latency\n speed-up", rotation=90, size=15, color="black")
    ax.set_ylim(0, 23)
    ax.set_xlabel(None)
    fig.tight_layout()
    #ax.set_yscale("log")

    bars = ax.patches
    patterns = ('/', '\\')
    hatches = [p for p in patterns for _ in range(6)]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)
    fig.tight_layout()

    rects = ax.patches
    # Make some labels.
    labels = list(round(df["COSTREAM"], 2)) + list(round(df["Flat Vector"], 2))
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 0.1, label, ha="center", va="bottom", fontsize=12, rotation=0)

    ax.legend(ncols=2, loc="upper center", facecolor='white', framealpha=1, frameon=True, fontsize=12)
    plt.savefig('optimizer_speedups.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--costream_results', default=None, required=True)
    parser.add_argument('--baseline_results', default=None, required=True)
    parser.add_argument('--costream_queries', default=None, required=True)  # directory of executed queries
    parser.add_argument('--baseline_queries', default=None, required=True)  # directory of executed queries
    parser.add_argument('--metric', choices=["proc-mean", "e2e-mean"], required=True)
    args = parser.parse_args()
    plot(args.costream_results, args.baseline_results, args.metric)
