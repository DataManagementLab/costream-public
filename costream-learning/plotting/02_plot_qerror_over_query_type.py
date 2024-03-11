import argparse

import matplotlib.pyplot as plt
import pandas as pd
import os

from learning.utils.utils import determine_query_type

plt.style.use('seaborn-v0_8')
QUERY_TYPES = {0: "Linear", 1: "2-Way-Join", 2: "3-Way-Join"}


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


def add_query_type_to_df(training_path: str, predictions: pd.DataFrame):
    missing = 0
    extended_rows = []
    # Readout hardware information from query graphs
    for index, row in predictions.iterrows():
        query_name = row["name"] + ".query"
        if os.path.exists(os.path.join(training_path, query_name)):
            row["query_type"] = determine_query_type(os.path.join(training_path, query_name))
            extended_rows.append(row)

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

    # Adding query type to the dataframes
    cost_preds = add_query_type_to_df(training_path, cost_preds)
    failing_preds = add_query_type_to_df(training_path, failing_preds)
    offset_preds = add_query_type_to_df(training_path, offset_preds)

    # Aggregate over predictions
    failing_preds_agg = failing_preds[["accuracy", "query_type"]].groupby("query_type", sort=False).agg("mean")
    offset_preds_agg = offset_preds[["accuracy", "query_type"]].groupby("query_type", sort=False).agg("mean")
    cost_preds_agg = cost_preds.groupby("query_type", sort=False)[["e2e", "proc-mean", "throughput"]].agg("median")

    # Merge to common dataframe
    combined_preds = pd.merge(failing_preds_agg, offset_preds_agg, left_index=True, right_index=True)
    combined_preds = combined_preds.rename(columns={"accuracy_x": "Query Success", "accuracy_y": "Backpressure"})
    combined_preds = pd.merge(combined_preds, cost_preds_agg, left_index=True, right_index=True)
    combined_preds = combined_preds.rename(columns={"e2e": "End-To-End-Lat.", "proc-mean": "Processing-Lat", "throughput": "Throughput"})
    combined_preds["Query Success"] = 100 * combined_preds["Query Success"]
    combined_preds["Backpressure"] = 100 * combined_preds["Backpressure"]

    custom_dict = {'Linear\nQuery': 0,
                   'Linear\nQuery\nwith Aggregation': 1,
                   '2-Way-Join\nQuery': 2,
                   '2-Way-Join\nQuery\nwith Aggregation': 3,
                   '3-Way-Join\nQuery': 4,
                   '3-Way-Join\nQuery\nwith Aggregation': 5}
    combined_preds = combined_preds.sort_index(key=lambda x: x.map(custom_dict))
    # Plot dataframes
    fig, axs = plt.subplots(2, 1, figsize=(8, 3), sharex="all")
    combined_preds[["End-To-End-Lat.", "Processing-Lat", "Throughput"]].plot(kind='bar', ax=axs[0], edgecolor="black")
    combined_preds[["Query Success", "Backpressure"]].plot(kind='bar', ax=axs[1], edgecolor="black", color=plt.rcParams['axes.prop_cycle'].by_key()['color'][4:6])

    # Add hatches
    bars = axs[0].patches
    patterns = ('//', "--", "\\\\")
    hatches = [p for p in patterns for i in range(len(cost_preds_agg))]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    bars = axs[1].patches
    patterns = ("--", '\\\\')
    hatches = [p for p in patterns for i in range(len(combined_preds))]
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)

    # Further settings, legends and labels
    axs[0].set_ylabel("Median\nQ-Error", rotation=90, fontsize=12, color="black")
    axs[0].set_ylim(bottom=1, top=1.8)
    axs[0].tick_params(axis='x', colors='black', rotation=0)
    axs[0].tick_params(axis='y', colors='black')

    axs[1].tick_params(axis='x', colors='black', rotation=0)
    axs[1].tick_params(axis='y', colors='black')
    axs[1].set_ylabel("Accuracy\n(%)", rotation=90, fontsize=12, color="black")
    axs[1].set_xlabel("Query Type", fontsize=12, color="black")

    axs[1].legend(ncols=2, loc="lower center", facecolor='white', framealpha=1, frameon=True)
    axs[0].legend(ncols=3, loc="lower center", facecolor='white', framealpha=1, frameon=True)

    plt.savefig('q_error_over_query_type.pdf', bbox_inches='tight')
    fig.tight_layout()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--best_models_path', default=None, required=True)
    parser.add_argument('--training_path', default=None, required=True)
    args = parser.parse_args()
    plot(args.best_models_path, args.training_path)
