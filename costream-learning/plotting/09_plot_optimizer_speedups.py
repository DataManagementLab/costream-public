import argparse
import pandas as pd
import os
import matplotlib.pyplot as plt

from learning.dataset.dataset_creation import extract_labels_from_directory

plt.style.use('seaborn-v0_8')
order = ['Linear', 'Linear\nwith Agg.', '2-Way-Join', '2-Way-J.\nwith Agg.', '3-Way-Join', '3-Way-J.\nwith Agg.']


def plot(prediction_results, metric):
    assert os.path.exists(prediction_results) and prediction_results.endswith(".csv")
    # Read prediction results
    df = pd.read_csv(prediction_results)
    df = df.set_index("query")

    # Read optimized query results
    labels = extract_labels_from_directory(queries_path=args.executed_queries,
                                           exclude_failing_queries=False,
                                           offset_to_boolean=True,
                                           stratify_offsets=False,
                                           stratify_failing=False,
                                           filters=None)

    labels.index = labels.index.str.replace('-optimized', '')
    df = pd.merge(df, labels[[metric, "offset"]], left_index=True, right_index=True)
    df["real_speedup"] = df["initial_true_value"] / df[metric]

    # Compute Failing Metrics
    failing = df[df[metric] == -1].groupby("query_type").count()[metric] / df.groupby("query_type").count()[metric]
    print('f{failing queries}', failing)

    # Compute Offset Queries
    offset = df[df["offset"] == True].groupby("query_type").count()["offset"] / df.groupby("query_type").count()[
        "offset"]
    print(offset)

    # removing failing queries,as no speed up is reported here
    df = df[df[metric] != -1]
    df.to_csv("combined_results.csv")

    # Compute real Speed-Ups
    df['query_type'] = df['query_type'].str.replace('Linear\nQuery', 'Linear')
    df['query_type'] = df['query_type'].str.replace('Linear\nwith Aggregation', 'Linear\nwith Agg.')
    df['query_type'] = df['query_type'].str.replace('2-Way-Join\nQuery', '2-Way-Join')
    df['query_type'] = df['query_type'].str.replace('2-Way-Join\nwith Aggregation', '2-Way-J.\nwith Agg.')
    df['query_type'] = df['query_type'].str.replace('3-Way-Join\nQuery', '3-Way-Join')
    df['query_type'] = df['query_type'].str.replace('3-Way-Join\nwith Aggregation', '3-Way-J.\nwith Agg.')
    real_speed_ups = df.groupby("query_type").agg('median')["real_speedup"]
    print(real_speed_ups)

    # Order bars
    real_speed_ups = real_speed_ups.iloc[real_speed_ups.index.map({o: i for i, o in enumerate(order)}).argsort()]
    cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

    # Plot bars
    # fig, ax = plt.subplots(1, 1, figsize=(7, 3))
    fig, ax = plt.subplots(1, 1, figsize=(8, 2.5))
    real_speed_ups.plot.bar(ax=ax, ecolor='black', capsize=10, edgecolor="black", color=cycle[1])
    ax.tick_params(axis='x', colors='black', rotation=0, labelsize=15)
    ax.tick_params(axis='y', colors='black', labelsize=15)
    ax.set_ylabel("Median latency\n speed-up", rotation=90, size=15, color="black")
    ax.set_ylim(0.1, 45)
    ax.set_xlabel(None)
    fig.tight_layout()
    ax.set_yscale("log")

    fig.tight_layout()

    rects = ax.patches
    # Make some labels.
    labels = list(round(real_speed_ups, 2))
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2, height * 1.1, label, ha="center", va="bottom",fontsize=12
        )

    plt.savefig('optimizer_speedups.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prediction_results', default=None, required=True)
    parser.add_argument('--executed_queries', default=None, required=True)  # directory of executed queries
    parser.add_argument('--metric', choices=["proc-mean", "e2e-mean"], required=True)
    args = parser.parse_args()
    plot(args.prediction_results, args.metric)
