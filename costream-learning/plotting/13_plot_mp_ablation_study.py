import argparse

import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

plt.style.use('seaborn-v0_8')


def plot(results):
    assert os.path.exists(results) and results.endswith(".csv")
    df = pd.read_csv(results)

    fig_width = 5
    fig, ax = plt.subplots(1, 1, figsize=(fig_width, 2))
    df.plot.barh(ax=ax, ecolor='black', capsize=5,  edgecolor="black", width=0.8, color= plt.rcParams['axes.prop_cycle'].by_key()['color'][4:6])
    ax.tick_params(axis='x', colors='black', rotation=0, labelsize=10)
    ax.tick_params(axis='y', colors='black', labelsize=8)
    ax.set_yticklabels(df["metric"], rotation=0, fontsize=10)
    ax.set_xlabel("Q-Error",  fontsize=10, color="black")
    ax.set_xscale("log")
    ax.set_xlim(left=1, right=45)
    # ax.axhline(1.5, color="black")
    # ax.axhline(3.5, color="black")
    ax.text(0.25, 0.00, "E2E-Latency", fontsize=10, rotation=0)
    ax.text(0.25, 2.3, "Proc.-Latency", fontsize=10, rotation=0)
    ax.text(0.25, 4.27, "Throughput", fontsize=10, rotation=0)
    ax.bar_label(ax.containers[0], label_type='edge', padding=3, fontsize=8)
    ax.bar_label(ax.containers[1], label_type='edge', padding=3, fontsize=8)
    ax.xaxis.set_label_coords(.9, -.05)
    ax2 = plt.axes([0, 0, 1, 1], facecolor=(1, 1, 1, 0))
    ax2.grid(False)
    x, y = np.array([[0.03, 0.97], [0.465, 0.465]])
    line = Line2D(x, y, lw=1., color='black', alpha=1)
    ax2.add_line(line)
    x, y = np.array([[0.03, 0.97], [0.685, 0.685]])
    line = Line2D(x, y, lw=1., color='black', alpha=1)
    ax2.add_line(line)


    bars = ax.patches
    patterns = ('///', '\\\\')
    hatches = [p for p in patterns for i in range(len(df))]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)
    ax.legend(fontsize=8, ncols=1)

    #ax.legend(fontsize=15, ncols=2)
    fig.tight_layout()
    plt.savefig('message_passing_ablation_study.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--results', default=None, required=True)
    args = parser.parse_args()
    plot(args.results)
