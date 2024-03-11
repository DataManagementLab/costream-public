import argparse
import matplotlib.pyplot as plt

from learning.constants import LABEL
from learning.dataset.dataset_creation import extract_labels_from_directory

titles = ["End-To-End-Latency", "Processing-Latency", "Throughput", "Delta-Queue"]
metrics = [LABEL.ELAT, LABEL.PLAT, LABEL.TPT, LABEL.BACKPRESSURE]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', default=None, required=True)
    parser.add_argument('--binary_offsets', default=True, required=False)
    args = parser.parse_args()

    # Read labels from disk
    labels = extract_labels_from_directory(args.dataset_path, exclude_failing_queries=True, offset_to_boolean=args.binary_offsets)

    # Number of all queries
    print("Number of all Queries: ", len(labels))

    # Look at offset distribution for classification
    if args.binary_offsets:
        print(labels.offset.value_counts(normalize=True))
        metrics.pop()
        titles.pop()

    for metric in metrics:
        print("---")
        print(f"Min-value for {metric}:\t{labels[metric].min():.2f}")
        print(f"Max-value for {metric}:\t{labels[metric].max():.2f}")
        print(f"80%-perc. for {metric}:\t{labels[metric].quantile(0.80):.2f}")

    """
    # Common plot
    fig, axs = plt.subplots(len(titles), 1, figsize=(6, 10))
    for (ax, title, metric) in zip(axs, titles, metrics):
        #for i, dff in labels.groupby("type"):
        ax.hist(labels[metric], bins=30, alpha=0.5, label=labels["type"].unique()[0])
        ax.set_title(title)
        ax.set_yscale("log")

    plt.legend()
    fig.tight_layout()
    plt.show()
    """
    # Plot divided by label distribution
    fig, axs = plt.subplots(len(titles), 1, figsize=(6, 10))
    for (ax, title, metric) in zip(axs, titles, metrics):
        for i, dff in labels.groupby("type"):
            ax.hist(dff[metric], bins=30, alpha=0.5, label=dff["type"].unique()[0])
            ax.set_title(title)
            ax.set_yscale("log")
    plt.legend()
    fig.tight_layout()
    plt.show()
