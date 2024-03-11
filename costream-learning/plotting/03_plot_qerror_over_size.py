import argparse
import os

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import StrMethodFormatter

plt.style.use('seaborn-v0_8')


def plot(path):
    assert os.path.exists(path)
    df = pd.read_csv(path)
    df["computation_time"] = 5 * df["size"] / 60
    fig, axs = plt.subplots(1, 2, figsize=(6, 2))

    axs[0].scatter(df["size"], df["q50"], label="Q50", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][4])
    axs[0].plot(df["size"], df["q50"], linestyle="-", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][4])

    axs[0].scatter(df["size"], df["q95"], label="Q95", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][5])
    axs[0].plot(df["size"], df["q95"], linestyle="-", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][5])
    axs[0].set_yscale('log')

    axs[1].scatter(df["size"], df["computation_time"], label="Computation Time (h)", c="black")
    axs[1].plot(df["size"], df["computation_time"], linestyle='-', c="black")
    axs[1].set_xscale('log')
    axs[1].set_ylabel("Exec. Time(h)", fontsize=15, color="black")
    axs[1].set_xlabel("Size of Dataset", fontsize=15, color="black")
    axs[1].tick_params(axis='x', colors='black', labelsize=15)
    axs[1].tick_params(axis='y', colors='black', labelsize=15)

    axs[0].set_xscale('log')
    axs[0].set_ylabel("Q-Error", rotation=90, fontsize=15, color="black")
    axs[0].set_xlabel("Size of Dataset", fontsize=15, color="black")
    axs[0].set_ylim(bottom=1)
    axs[0].legend(fontsize=10, ncols=2,  facecolor='white', framealpha=1, frameon=True)
    axs[0].tick_params(axis='x', colors='black', labelsize=15)
    axs[0].tick_params(axis='y', colors='black', labelsize=15)
    axs[0].yaxis.set_major_formatter(StrMethodFormatter('{x:.0f}'))
    fig.tight_layout()
    plt.savefig('q_error_over_size.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--result_path', default=None, required=True)
    args = parser.parse_args()
    plot(args.result_path)