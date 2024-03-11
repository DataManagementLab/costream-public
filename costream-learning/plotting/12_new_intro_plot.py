import argparse
import matplotlib.pyplot as plt
import numpy as np

plt.style.use('seaborn-v0_8')
titles={"e2e-latency": "End-To-End-Latency", "proc-latency": "Processing Latency", "throughput": "Throughput"}


def plot():
    species = ("Seen\nqueries", "Unseen\nhardware", "Unseen\nqueries", "Unseen\nbenchmark")
    means = {
           'COSTREAM': (1.37, 1.59,  2.17,   1.41),
        'Flat Vector': (13.28, 63.79, 444.03, 17.15),
    }

    x = np.arange(len(species))  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(figsize=(7, 2.5))
    ax.set_yscale("log")

    # colors = [plt.rcParams['axes.prop_cycle'].by_key()['color'][2], plt.rcParams['axes.prop_cycle'].by_key()['color'][5]]
    for (attribute, measurement) in means.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width,  edgecolor="black", label=attribute)
        ax.bar_label(rects, padding=0)
        multiplier += 1

    ax.tick_params(axis='x', colors='black', rotation=0, labelsize=14)
    ax.tick_params(axis='y', colors='black', rotation=0, labelsize=14)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Median Q-Error', color="black", size=14)
    ax.set_xticks(x + width, species)
    ax.axvline(0.65, color="black")
    #ax.text(0.35, 21, "Seen Queries", fontsize=10,  verticalalignment='top')
    #ax.text(2.35, 21, "Unseen Queries", fontsize=10,  verticalalignment='top')
    ax.set_ylim(0, 1000)

    bars = ax.patches
    patterns = ('//', '\\')
    hatches = [p for p in patterns for i in range(4)]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    ax.legend(loc='upper center', ncols=1, fontsize=10, bbox_to_anchor=(0.38, 1.03))
    fig.tight_layout()
    #plt.show()
    plt.savefig("motivating_plot.pdf")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    plot()
