import argparse
import pandas as pd
import os
import matplotlib.pyplot as plt


plt.style.use('seaborn-v0_8')
titles={"e2e-latency": "End-To-End-Latency", "proc-latency": "Processing Latency", "throughput": "Throughput"}

def plot(results):
    assert os.path.exists(results) and results.endswith(".csv")
    df = pd.read_csv(results)
    fig, axs = plt.subplots(1,2, figsize=(8, 2), sharex="col")


    for i, (metric, dff) in enumerate(df.groupby("metric")):
        if metric == "throughput":
            dff.index = dff["type"]
            color1 = plt.rcParams['axes.prop_cycle'].by_key()['color'][2]
            color2 = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
            colors = [color1, color2]
            for i, m in enumerate(["Q50", "Q95"]):
                dfff = dff[[m+"-initial", m + "-retrain"]]
                dfff.plot(kind='bar', ax=axs[i], legend=None,  edgecolor='black',  width=0.9, color=colors)
                """
                dff.plot.bar(x="type", y="Q95-initial", ax=axs[1], legend=0, label="Q95 (initial)", color=color1,
                             edgecolor='black', capsize=10)
    
                dff.plot.bar(x="type", y="Q95-retrain", ax=axs[1], legend=0, label="Q95 (retrained)", color=color2,
                             edgecolor='black', capsize=10)
    
                dff.plot.bar(x="type", y="Q50-initial", ax=axs[0], color=color1, legend=0, label="Q50 (initial)",
                             edgecolor='black', capsize=10)
    
                dff.plot.bar(x="type", y="Q50-retrain", ax=axs[0], color=color2, legend=0, label="Q50 (retrained)",
                             edgecolor='black', capsize=10)
                """
                for container in axs[i].containers:
                    axs[i].bar_label(container, rotation=0)

            axs[0].set_ylabel("")
            axs[0].set_xlabel("")
            axs[1].set_ylabel("")
            axs[1].set_xlabel("")

            axs[0].set_yscale("log")
            axs[1].set_yscale("log")
            axs[1].set_ylabel("Q95", fontsize=12, color="black")
            axs[0].set_ylabel("Q50", fontsize=12, color="black")
            #axs[0].yaxis.set_minor_formatter(mticker.ScalarFormatter())
            #axs[0].yaxis.set_major_formatter(mticker.ScalarFormatter())
            axs[0].yaxis.set_minor_locator(plt.MaxNLocator(1))
            axs[0].yaxis.set_ticks([1, 5, 10], labels=[1, 5, 10])
            #axs[0].set_title(titles[metric], fontsize=12)
            axs[0].set_ylim(bottom=1, top=10)
            axs[1].set_ylim(bottom=1, top=1000)
            axs[1].tick_params(axis='x', colors='black', rotation=0)
            axs[0].tick_params(axis='x', colors='black', rotation=0)
            axs[1].tick_params(axis='y', colors='black')

    fig.tight_layout()
    fig.align_labels()

    for plot in [0, 1]:
        bars = axs[plot].patches
        patterns = ('//', '\\\\')
        hatches = [p for p in patterns for i in range(3)]
        print(hatches)
        for bar, hatch in zip(bars, hatches):
            bar.set_hatch(hatch)

    axs[1].legend(["initial", "retrained"], bbox_to_anchor=[1.02, 0.65])

    fig.tight_layout()
    plt.savefig('retraining_experiment.pdf', bbox_inches='tight')
    plt.show()

    """
      for i, (metric, dff) in enumerate(df.groupby("metric")):
        dff.index = dff["type"]

        dff.plot(x="type", y="Q95-initial", ax=axs[0,i],
                 c=color1, linestyle="--", legend=0, label="Q95 (initial)",marker="x")

        dff.plot(x="type", y="Q95-retrain", ax=axs[0, i],
                 c=color2, linestyle="--", legend=0, label="Q95 (retrained)", marker="^")

        dff.plot(x="type", y="Q50-initial", ax=axs[1, i],
                 c=color1, linestyle="--", legend=0, label="Q50 (initial)", marker="x")

        dff.plot(x="type", y="Q50-retrain", ax=axs[1, i],
                 c=color2, linestyle="--", legend=0, label="Q50 (retrained)", marker="^")

        axs[0, i].set_ylabel("")
        axs[1, i].set_ylabel("")
        axs[1, i].set_xlabel("")

        axs[0, 0].set_ylabel("Q95", fontsize=12, color="black")
        axs[1, 0].set_ylabel("Q50", fontsize=12, color="black")

        axs[0, i].set_title(titles[metric], fontsize=12)
        axs[0, i].set_ylim(bottom=1)
        axs[0, i].set_ylim(bottom=1, top=axs[0, i].get_ylim()[1]*1.3)
        axs[0, i].tick_params(axis='x', colors='black', rotation=0)
        axs[0, i].tick_params(axis='y', colors='black')
        axs[0, i].set_yscale("log")

    fig.tight_layout()
    fig.align_labels()

    handles, labels = axs[1, 0].get_legend_handles_labels()
    labels = ["initial", "retrained"]
    fig.legend(handles, labels, loc='center right',ncol=1, bbox_to_anchor=(1.12, 0.5), fontsize=8)
    plt.savefig('retraining_experiment.pdf', bbox_inches='tight')
    plt.show()"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--results', default=None, required=True)
    args = parser.parse_args()
    plot(args.results)
