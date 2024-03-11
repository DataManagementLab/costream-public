import argparse
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

titles = ["End-To-End-Latency", "Processing-Latency", "Throughput", "Delta-Queue"]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-path', default=None, required=True)
    args = parser.parse_args()

    if not os.path.exists(args.csv_path):
        raise RuntimeError("Path does not exist")

    df = pd.read_csv(args.csv_path)
    fig, ax = plt.subplots(1, 1, figsize=(6, 6))
    ax.set_ylim(0, 6)
    ax.scatter(df["real_value"], np.log(df["qerror"]))
    ax.set_title("Q-Error over Offsets")
    ax.set_ylabel("Log(Q-Error)", rotation=90, fontsize=10, color="black")
    ax.set_xlabel("Real Value (ms)", fontsize=10, color="black")
    ax.tick_params(axis='x', colors='black')
    ax.tick_params(axis='y', colors='black')
    fig.tight_layout()
    plt.show()