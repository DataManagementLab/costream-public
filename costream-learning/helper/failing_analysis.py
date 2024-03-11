import argparse
import os

import pygraphviz as pgv
import networkx as nx
import tqdm


def analyze_data(path: str):
    for graph in tqdm.tqdm([f for f in os.listdir(path) if f.endswith(".query")]):
        g = nx.DiGraph(pgv.AGraph(os.path.join(path, graph)))
        failing = False
        for node in g.nodes(data=True):
            _, node_data = node
            if "throughput-mean" in node_data.keys() and node_data["throughput-mean"] == "-1":
                failing = True

        if failing:
            selectivities = []
            print(f'Failing query {graph}')
            for node in g.nodes(data=True):
                _, node_data = node
                if "realSelectivity" in node_data.keys():
                    selectivities.append(node_data["realSelectivity"])
            print(selectivities)
            #if node_data["operatorType"] in ["filter", "join", "windowedAggregation", "aggregation"]:
            #    if "realSelectivity" not in node_data.keys():
            #        defect_queries.add(graph)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, required=True)
    args = parser.parse_args()
    analyze_data(args.path)

