import datetime
import os

import argparse
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pygraphviz as pgv
import pandas as pd

plt.style.use('ggplot')


class Param:
    EVENT_RATE = "confEventRate"
    COUNTER = "counter"
    RAM_USAGE = "ram_usage"
    CPU_USAGE = "cpu_usage"
    JVM_SIZE = "jvm_size"
    NETWORK_USAGE = "network-usage"
    ALL = [EVENT_RATE, COUNTER, RAM_USAGE, CPU_USAGE, JVM_SIZE, NETWORK_USAGE]


class SpoutParam:
    OFFSET = "offset"
    PROD_INTERVAL = "producer-interval"
    INGEST_INTERVAL = "ingestion-interval"
    KAFKA_DUR = "kafkaDuration"
    TOPIC = "topic"
    ALL = [OFFSET, PROD_INTERVAL, INGEST_INTERVAL, KAFKA_DUR]


class Label:
    TPT = "throughput-mean"
    E2E = "e2e-mean"
    PROC = "proc-mean"
    DELTA = "deltaQueue-mean"
    #ALL = [TPT, E2E, PROC, DELTA, SpoutParam.PROD_INTERVAL, SpoutParam.INGEST_INTERVAL]
    ALL = [TPT, PROC, E2E, DELTA]


FULL_QUERY_NAMES = {"linear-agg-duration": "Linear aggregation Query (Duration)",
                    "linear": "Linear query",
                    "linear-agg-count": "Linear aggregation Query (Count)",
                    "two-way-join": "Two-Way-Join",
                    "three-way-join": "Three-Way-Join"}

QUERY_ATTRIBUTES = ["duration",
                    "cluster",
                    "mode",
                    "jvmSize",
                    "common-bandwidth",
                    "common-ram",
                    "common-cpu",
                    "common-latency",
                    "windowLength",
                    "tupleWidth"]

PLOT_NAMES = ['Throughput (ev/s)',
              'Processing Lat.(s)',
              'End-To-End Lat.(s)',
              'Backpress. Rate(#/s)',
              "Producer Rate(ev/s)",
              "Ingestion Rate(ev/s)"]


def extract_metrics_from_single_graph(path, filename):
    metric = dict()
    topics = set()

    graph = nx.DiGraph(pgv.AGraph(os.path.join(filename, path)).to_directed())
    metric["id"] = path.split(".")[0]

    for attr in QUERY_ATTRIBUTES:
        try:
            attribute = graph.graph[attr]
            try:
                attribute = float(attribute)
            except ValueError:
                pass
            metric[attr] = attribute
        except KeyError:
            pass

    # Extract different topics of query
    for node_key, node_data in graph.nodes(data=True):
        if SpoutParam.TOPIC in node_data:
            topic = int(node_data[SpoutParam.TOPIC][-1])
            topics.add(topic)

    for node_key, node_data in graph.nodes(data=True):
        for m in Param.ALL + SpoutParam.ALL + Label.ALL:
            if m in node_data:
                key = m
                # special handling of spouts, adding enumeration suffix
                if m in SpoutParam.ALL and len(topics) > 1:
                    key = node_data[SpoutParam.TOPIC] + "-" + m
                try:
                    metric[key] = float(node_data[m])
                except ValueError:
                    metric[key] = None
    return metric, topics


def extract_metrics_from_graph(directory):
    print("Reading Metrics from dir:", directory)
    if not os.path.exists(directory) or os.listdir(directory) == []:
        print("No metrics found at:", directory)
        return None

    query_metrics = []
    query_topics = set()

    for graph_path in [g for g in os.listdir(directory) if g.endswith(".query")]:
        m, t = extract_metrics_from_single_graph(graph_path, directory)
        query_topics.update(t)
        query_metrics.append(m)

    query_metrics = pd.DataFrame(query_metrics).sort_values(by=[Param.EVENT_RATE])
    query_metrics[Label.E2E] = query_metrics[Label.E2E] / 1000
    query_metrics[Label.PROC] = query_metrics[Label.PROC] / 1000
    query_metrics["common-ram"] = query_metrics["common-ram"] / 1000

    if len(query_topics) == 1:
        query_metrics[Label.DELTA] = query_metrics[SpoutParam.OFFSET] / (query_metrics[SpoutParam.KAFKA_DUR] * 0.001)
        query_metrics[SpoutParam.PROD_INTERVAL] = 1 / (query_metrics[SpoutParam.PROD_INTERVAL]) * 1000
        query_metrics[SpoutParam.INGEST_INTERVAL] = 1 / (query_metrics[SpoutParam.INGEST_INTERVAL]) * 1000
    else:
        query_metrics[Label.DELTA] = query_metrics["source0-" + SpoutParam.OFFSET] / (query_metrics["source0-" + SpoutParam.KAFKA_DUR] * 0.001)
        query_topics.remove(0)

        # Adding up multiple sources
        for topic in query_topics:
            query_metrics[Label.DELTA] += query_metrics["source" + str(topic) + "-offset"] \
                                          / (query_metrics["source" + str(topic) + "-kafkaDuration"] * 0.001)

    # removing -1 encoded values as these are failed queries
    query_metrics = query_metrics[query_metrics["throughput-mean"] != -1]
    return query_metrics


def set_ax_titles(axs):
    if len(axs.shape) > 1:
        for ax, cluster_name in zip(list(axs.flat), PLOT_NAMES):
            ax.set_ylabel(cluster_name, rotation=90, fontsize=10, color="black")
            #ax.get_yaxis().set_label_coords(-0.18, 0.5)
            ax.tick_params(axis='x', colors='black')
            ax.tick_params(axis='y', colors='black')
    else:
        for ax, plot_name in zip(axs, PLOT_NAMES):
            #ax.set_title(plot_name, fontsize=10)
            ax.get_yaxis().set_label_coords(-0.18, 0.5)
            ax.xaxis.label.set_color('black')
            ax.tick_params(axis='x', colors='black')
            ax.tick_params(axis='y', colors='black')


def visualize_usage_metrics(metrics, configs):
    horizontally = True
    for queryType, type_df in metrics.groupby("mode"):
        figsize = (3 * len(list(metrics.cluster.unique())), 5 * len(Label.ALL))
        dims = len(Label.ALL), len(list(metrics.cluster.unique()))
        if horizontally:
            dims = list(reversed(dims))
            figsize = list(reversed(figsize))
        fig, axs = plt.subplots(2, 2, figsize=(7, 4), sharex="col")

        if len(axs.shape) == 1:
            axs = np.expand_dims(axs, axis=1)

        if configs["axis"] == "confEventRate":
            text = 'Source Event Rate (ev/s)'
        elif configs["axis"] == "windowLength":
            text = "Window Length (s)"
        elif configs["axis"] == "tupleWidth":
            text = "Tuple Width"
        fig.text(0.5, 0.005, text, ha='center', fontsize=10)
        set_ax_titles(axs)

        type_df = type_df.sort_values(by=[configs["axis"], configs["name"]])
        for idx, (cluster, cluster_df) in enumerate(type_df.groupby("cluster")):
            for (value, df), marker in zip(cluster_df.groupby(configs["name"]), ["x", "+", "v", "*"]):
                for row, metric in enumerate(["throughput-mean", "e2e-mean"]):
                    axs[row, 0].plot(df[configs["axis"]], df[metric], linestyle="-")
                    axs[row, 0].scatter(df[configs["axis"]], df[metric], label=str(value) + configs["unit"],
                                          marker=marker)
                for row, metric in enumerate(["proc-mean", "offset"]):
                    axs[row, 1].plot(df[configs["axis"]], df[metric], linestyle="-")
                    axs[row, 1].scatter(df[configs["axis"]], df[metric], label=str(value) + configs["unit"],
                                          marker=marker)

        handles_0, labels_0 = axs[1, -1].get_legend_handles_labels()
        fig.legend(handles_0, labels_0, loc='upper center', ncols=4, bbox_to_anchor=(0.5, 1.05), columnspacing=0.7)
        fig.align_labels()
        plt.tight_layout()
        plt.savefig('query_costs_over_event_rate.pdf', bbox_inches='tight')
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', default=None, required=True)
    args = parser.parse_args()

    if "ram" in args.dataset_path:
        configs = dict(name="common-ram", unit="GB", axis="confEventRate", axisName="Event Rate", variation="RAM")

    elif "latency" in args.dataset_path:
        configs = dict(name="common-latency", unit="ms", axis="confEventRate", axisName="Event Rate",
                       variation="Latency")

    elif "cpu" in args.dataset_path:
        configs = dict(name="common-cpu", unit="%", axis="confEventRate", axisName="Event Rate", variation="CPU")

    elif "bandwidth" in args.dataset_path:
        configs = dict(name="common-bandwidth", unit="Mbit/s", axis="confEventRate", axisName="Event Rate", variation="Bandwidth")

    elif "join" in args.dataset_path:
        configs = dict(name="common-ram", unit="GB", axis="confEventRate", axisName="Event Rate", variation="RAM")

    metrics = extract_metrics_from_graph(args.dataset_path)
    if not metrics.empty:
        visualize_usage_metrics(metrics, configs)
    else:
        raise Exception("Could not extract metrics from path")
