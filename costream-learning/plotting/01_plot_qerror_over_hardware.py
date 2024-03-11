import argparse
import statistics

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import pygraphviz as pgv
import os

from matplotlib.text import Text

plt.style.use('seaborn-v0_8')
HARDWARE_FEATURES = ["cpu", "ram", "bandwidth", "latency"]
units = ["%", "MB", "mbit/s", "ms"]
titles = dict(cpu="CPU (%)", ram="RAM (MB)", bandwidth="Bandwidth (mbit/s)", latency="Latency (ms)")
bins = [
    [0, 100, 200, 300, 400, 500, 600, 700, 800],
    [0, 1000, 2000, 4000, 8000, 16000, 24000, 32000],
    [0, 25, 100, 200, 400, 800, 1600, 3200, 6400, 10000],
    [1, 2, 5, 10, 20, 40, 80, 160]
]

percentiles = False


def read_out_single_prediction(best_model_paths, path):
    model_path = os.path.join(best_model_paths, path)
    prediction_path = None
    if os.path.isdir(model_path):
        for file_path in os.listdir(model_path):
            if "pred" in file_path and file_path.endswith(".csv"):
                prediction_path = os.path.join(model_path, file_path)
    if prediction_path:
        assert os.path.exists(prediction_path)
        predictions = pd.read_csv(prediction_path)
        for m in ["e2e", "offset", "throughput", "proc-mean", "failing"]:
            if m in prediction_path:
                metric = m
    return predictions, metric


def add_hardware_range_to_df(training_path: str, predictions: pd.DataFrame()):
    extended_rows = []
    for index, row in predictions.iterrows():
        query_name = row["name"] + ".query"
        if os.path.exists(os.path.join(training_path, query_name)):
            graph = nx.DiGraph(pgv.AGraph(os.path.join(training_path, query_name)).to_directed())
            for metric in HARDWARE_FEATURES:
                values = []
                for node, data in graph.nodes(data=True):
                    if data["operatorType"] == "host":
                        values.append(int(float(data[metric])))
                row[metric] = statistics.mean(values)
            extended_rows.append(row)

    # Create extended dataframe
    return pd.DataFrame.from_dict(extended_rows)


def plot(best_models_path, training_path):
    # Collect and merge all predictions into dataframes
    cost_preds = pd.DataFrame()
    failing_preds = pd.DataFrame()
    offset_preds = pd.DataFrame()

    for path in os.listdir(best_models_path):
        print(path)
        predictions, metric = read_out_single_prediction(best_models_path, path)
        if metric == "failing":
            failing_preds = predictions[["name", "accuracy"]]
        elif metric == "offset":
            offset_preds = predictions[["name", "accuracy"]]
        else:
            predictions[metric] = predictions["qerror"]
            predictions.drop(["real_value", "prediction", "qerror", "accuracy"], errors='ignore', inplace=True, axis=1)
            if cost_preds.empty:
                cost_preds = predictions
            else:
                cost_preds = pd.merge(cost_preds, predictions, on='name')

    # Add Hardware ranges to the DF and rename columns
    cost_preds = add_hardware_range_to_df(training_path, cost_preds)
    cost_preds = cost_preds.rename(columns={"e2e": "End-To-End-Lat.", "proc-mean": "Processing-Lat", "throughput": "Throughput"})
    failing_preds = add_hardware_range_to_df(training_path, failing_preds)
    offset_preds = add_hardware_range_to_df(training_path, offset_preds)
    combined_preds = pd.merge(failing_preds, offset_preds, on=["cpu", "ram", "bandwidth","latency", "name"], how='outer')
    combined_preds = combined_preds.rename(columns={"accuracy_x": "Query Success", "accuracy_y": "Backpressure"})
    combined_preds["Query Success"] = 100 * combined_preds["Query Success"]
    combined_preds["Backpressure"] = 100 * combined_preds["Backpressure"]

    # Iterate over hardware features and generate cuts for these
    for bin, feat in zip(bins, HARDWARE_FEATURES):
        cost_preds[feat] = pd.cut(cost_preds[feat], bins=np.array(bin), include_lowest=False, retbins=False)
        combined_preds[feat] = pd.cut(combined_preds[feat], bins=np.array(bin), include_lowest=False, retbins=False)

    # Create plot by enumerating over hardware features
    fig, axs = plt.subplots(2,4, figsize=(8, 3), sharex="col", sharey="row")
    for i, feat in enumerate(HARDWARE_FEATURES):
        cost_preds_agg = cost_preds.groupby(feat, sort=True)[["End-To-End-Lat.", "Processing-Lat", "Throughput"]].agg("median").dropna()
        combined_preds_agg = combined_preds.groupby(feat, sort=True)[["Query Success", "Backpressure"]].agg("mean").dropna()
        cost_preds_agg.plot(kind='bar', ax=axs[0, i], edgecolor="black", legend=False,  align="edge", width=1)
        combined_preds_agg.plot(kind='bar', ax=axs[1, i], edgecolor="black", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][4:6], legend=False,  align="edge", width=1)

        axs[0, i].tick_params(axis='x', colors='black', rotation=45)
        axs[0, i].tick_params(axis='y', colors='black')
        axs[0, i].set_ylim(bottom=1, top=2)

        axs[1, i].tick_params(axis='x', colors='black', rotation=45)
        axs[1, i].tick_params(axis='y', colors='black')
        axs[1, i].set_xlabel(titles[feat], rotation=0, fontsize=12, color="black")

        ticks = list(axs[1, i].get_xticks())
        tick_labels = list(axs[1, i].get_xticklabels())
        axs[1, i].set_xticks(ticks + [axs[1, i].get_xticks()[-1] + 1])
        new_tick_labels = []
        for label in tick_labels:
            new_tick_labels.append(Text(label._x, 0, label._text.split(",")[0].replace("(", "")))
        new_tick_labels.append(Text(tick_labels[-1]._x, 0, tick_labels[-1]._text.split(",")[1].replace("]", "")))
        axs[1, i].set_xticklabels(new_tick_labels)

        # Add hatches
        bars = axs[0, i].patches
        patterns = ('//', "--", "\\\\")
        hatches = [p for p in patterns for i in range(len(cost_preds_agg))]
        for bar, hatch in zip(bars, hatches):
            bar.set_hatch(hatch)
        bars = axs[1, i].patches
        patterns = ("--", '\\\\')
        hatches = [p for p in patterns for i in range(len(combined_preds_agg))]
        for bar, hatch in zip(bars, hatches):
            bar.set_hatch(hatch)

    axs[1, 0].set_ylabel("Accuracy\n(%)", rotation=90, fontsize=12, color="black")
    axs[0, 0].set_ylabel("Median\nQ-Error", rotation=90, fontsize=12, color="black")

    # Create common legend
    handles_0, labels_0 = axs[1, -1].get_legend_handles_labels()
    handles_1, labels_1 = axs[0, -1].get_legend_handles_labels()
    fig.align_labels()
    axs[0, 0].set_zorder(-1)
    axs[0, 1].set_zorder(-2)
    axs[0, 2].set_zorder(-3)
    axs[0, 3].set_zorder(-4)
    axs[1, 0].set_zorder(-5)
    axs[1, 1].set_zorder(-6)
    axs[1, 2].set_zorder(-7)
    axs[1, 3].set_zorder(-8)

    axs[0, 0].legend(handles_1, labels_1, loc='lower center', ncols=3, bbox_to_anchor=(2.5, -0.05), framealpha=1, frameon=True, facecolor='white',)
    axs[1, 0].legend(handles_0, labels_0, loc='lower center', ncols=3, bbox_to_anchor=(2.6, -0.05), framealpha=1, frameon=True, facecolor='white',)
    plt.savefig('q_error_over_hardware.pdf', bbox_inches='tight')
    plt.show()
    """
    # Also draw motivating example
    fig, axs = plt.subplots(1, 1, figsize=(8, 4))
    cost_preds_agg = cost_preds.groupby("cpu", sort=True)[["End-To-End-Lat.", "Processing-Lat", "Throughput"]].agg("median").dropna()
    cost_preds_agg.plot(kind='bar', ax=axs, edgecolor="black", legend=False, align="edge", width=1)

    axs.tick_params(axis='x', colors='black', rotation=45)
    axs.tick_params(axis='y', colors='black')
    axs.set_ylim(bottom=1, top=1.8)
    axs.set_xlabel(titles["cpu"], rotation=0, fontsize=12, color="black")

    ticks = list(axs.get_xticks())
    tick_labels = list(axs.get_xticklabels())
    axs.set_xticks(ticks + [axs.get_xticks()[-1] + 1])
    new_tick_labels = []
    for label in tick_labels:
        new_tick_labels.append(Text(label._x, 0, label._text.split(",")[0].replace("(", "")))
    new_tick_labels.append(Text(tick_labels[-1]._x, 0, tick_labels[-1]._text.split(",")[1].replace("]", "")))
    axs.set_xticklabels(new_tick_labels)

    axs.set_ylabel("Cost Estimation Error \n(Median Q-Error)", rotation=90, fontsize=12, color="black")

    # Add hatches
    bars = axs.patches
    patterns = ('///', '..', 'xxx')
    hatches = [p for p in patterns for i in range(len(cost_preds_agg))]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)
    #fig.legend(loc='upper center', ncols=3, bbox_to_anchor=(0.5, 1.07))

    plt.savefig('motivating_plot.pdf', bbox_inches='tight')
    plt.show()
    """

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--best_models_path', default=None, required=True)
    parser.add_argument('--training_path', default=None, required=True)
    args = parser.parse_args()
    plot(args.best_models_path, args.training_path)
