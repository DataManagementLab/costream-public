import argparse
import pandas as pd
import os
import matplotlib.pyplot as plt

plt.style.use('seaborn-v0_8')


def plot(results):
    assert os.path.exists(results) and results.endswith(".csv")
    df = pd.read_csv(results)
    df = df.rename(columns={"q50": "Q50", "q95": "Q95"})
    print(df)

    order = ["Query Nodes", "+ HW Nodes", "+ HW Features"]
    mapping = {o: i for i, o in enumerate(reversed(order))}
    key = df['mode'].map(mapping)
    df['mode'] = df['mode'].replace("Query Nodes", "Query\nNodes")
    df['mode'] = df['mode'].replace("+ HW Features", "+ HW\nFeatures")
    df['mode'] = df['mode'].replace("+ HW Nodes", "+ HW\nNodes")

    df = df.iloc[key.argsort()]
    df = df.set_index("mode")

    fig, ax = plt.subplots(1, 1, figsize=(6.3, 2.8))
    df.plot.barh(ax=ax, ecolor='black', capsize=10,  edgecolor="black", width=0.8, color= plt.rcParams['axes.prop_cycle'].by_key()['color'][4:6])
    ax.tick_params(axis='x', colors='black', rotation=0, labelsize=15)
    ax.tick_params(axis='y', colors='black', labelsize=15)
    ax.set_xlabel("Q-Error",  fontsize=15, color="black")
    ax.set_ylabel("Featurization",  fontsize=15, color="black")
    ax.set_xlim(left=1, right=150)
    ax.set_xscale("log")
    ax.bar_label(ax.containers[0], label_type='edge', padding=3, fontsize=13)
    ax.bar_label(ax.containers[1], label_type='edge', padding=3, fontsize=13)

    bars = ax.patches
    patterns = ('/', '\\')
    hatches = [p for p in patterns for i in range(len(df))]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    ax.legend(fontsize=15)
    fig.tight_layout()
    plt.savefig('ablation_study.pdf', bbox_inches='tight')
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--results', default=None, required=True)
    args = parser.parse_args()
    plot(args.results)
