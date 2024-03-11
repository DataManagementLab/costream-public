import os
import random
import shutil
import sys

import networkx as nx
import numpy as np
import pandas as pd
from networkx.drawing.nx_agraph import write_dot

import argparse
import pygraphviz as pgv
from tqdm import tqdm

from baseline import preprocess_dataset_baseline, Featurization
from learning.constants import LABEL
from learning.dataset.dataset import GraphDataset
from learning.dataset.dataset_creation import load_graphs_from_disk
from learning.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from learning.utils.utils import determine_query_type
import lightgbm as lgb


def parse_query(query: os.path, clean: bool, metric: str):
    """
    Convert a path to a query
    Returns initial_query but remove labels from it
    Returns a sub-graph of hosts, a sub-graph of operators and the initial latency
    Optionally removes data characteristics and labels to prepare an optimized query for execution
    """
    if not os.path.isfile(query) or not query.endswith(".query"):
        raise Exception("Input query not a valid dotfile")

    # Convert to graph
    initial_query = nx.DiGraph(pgv.AGraph(query))
    hosts = []

    initial_value = None
    # Split up hosts from operator graph
    for node, data in initial_query.nodes(data=True):
        if data["operatorType"] == "host":
            hosts.append((node, data))
        if data["operatorType"] == "sink":
            initial_value = float(data[metric])
        if clean:
            for k in (["e2e-mean", "proc-mean", "throughput-mean", "offset", "realSelectivity",
                       "outputRate", "inputRate", "counter", "inputCounter", "outputCounter",
                       "query"]):
                data.pop(k, None)

    # Remove hosts from graph
    operators = initial_query.copy()
    for host, _ in hosts:
        operators.remove_node(host)

    return initial_query, hosts, operators, initial_value


def enumerate_placements(hosts: list, operator_graph: nx.DiGraph, num: int):
    enumerated_graphs = []
    for i in range(int(num)):
        current_graph = operator_graph.copy()

        # Constraint: Consider storm internal groupings
        groups = dict()
        operators_wo_group = []
        for operator, operator_attributes in current_graph.nodes(data=True):
            if "grouping" in operator_attributes.keys():
                group = operator_attributes["grouping"]
                if group not in groups.keys():
                    groups[group] = []
                groups[group].append(operator)
            else:
                operators_wo_group.append(operator)

        # Add pseudo-groups for lone operators
        i = 10
        for op in operators_wo_group:
            groups["group" + str(i)] = [op]
            i += 1

        placements = []
        # Constraint: Avoid Ping-Pong in candidates to prevent flowing data back and forth
        for operators in groups.values():
            placement_found = False
            while not placement_found:
                random_host = random.choice(hosts)
                placement_found = check_potential_placement(operators, random_host, current_graph)
            placements.append((operators, random_host))
            # actually place the operator group
            for o in operators:
                current_graph.add_node(random_host[0], **random_host[1])
                current_graph.add_edge(o, random_host[0])

        # Add network data flow to candidate graph
        edges_to_add = []
        for operator, operator_attributes in current_graph.nodes(data=True):
            if operator_attributes["operatorType"] != "host":
                host = get_host_for_operator(operator, placements)
                sucessor, sucessor_host = None, None

                # identify successor host
                for edge in current_graph.edges:
                    if edge[0] == operator and current_graph.nodes[edge[1]]["operatorType"] != "host":
                        sucessor = edge[1]

                if sucessor:
                    sucessor_host = get_host_for_operator(sucessor, placements)
                    if host != sucessor_host:
                        edges_to_add.append((host, sucessor_host))

        for edge in edges_to_add:
            current_graph.add_edge(*edge)

        enumerated_graphs.append(current_graph)

    return enumerated_graphs


def check_potential_placement(operators: list, host: str, graph):
    # If host was not yet assigned, then take any host
    host_existing = False
    for node, _ in graph.nodes(data=True):
        if node == host[0]:
            host_existing = True

    if not host_existing:
        return True

    # If host is already assigned, check if already-existing operators are neighbors
    else:
        for edge in graph.edges():
            # identify potential neighbors
            if edge[1] == host[0]:
                # if at least one neighbor has a direct edge to the current operators, accept the placement
                for o in operators:
                    if graph.has_edge(edge[0], o) or graph.has_edge(o, edge[0]):
                        return True

    return False


def get_random_host(graph):
    hosts = []
    for operator, operator_attributes in graph.nodes(data=True):
        if operator_attributes["operatorType"] == "host":
            hosts.append(operator)

    assert len(hosts) > 0
    return random.choice(hosts)


def get_host_for_operator(operator_id, placement_list):
    results = []
    for placement in placement_list:
        if operator_id in placement[0]:
            results.append(placement[1][0])
    assert len(results) == 1
    return results[0]


def write_graphs_to_disk(enums: list, path: str, query_name: str, candidate: bool):
    os.makedirs(path, exist_ok=True)
    for i, graph in enumerate(enums):
        if candidate:
            name = query_name + "-" + str(i) + "-candidate"
        else:
            name = query_name
        write_dot(graph, os.path.join(path, name + ".query"))


def optimize_single_query(input_path, output_path, query_file, args, feature_statistics):
    # Create output directory
    os.makedirs(output_path, exist_ok=True)

    query_name = os.path.split(query_file)[-1].split(".")[0]

    # Split up query to list of hosts and operator-subgraph
    initial_query, hosts, operator_graph, initial_value = parse_query(os.path.join(input_path, query_file),
                                                                      clean=False,
                                                                      metric=args.metric)
    # add initial query to enumerations
    enumerations = [initial_query]

    # Create a set of random enumerations
    enumerations = enumerations + enumerate_placements(hosts, operator_graph, args.num)

    # Write enumerations to disk
    shutil.rmtree("./tmp", ignore_errors=True)
    write_graphs_to_disk(enumerations, "./tmp", query_name, candidate=True)

    model = lgb.Booster(model_file='../experimental_data/09_baseline_models/baseline_proc-mean.txt')

    loader_args = dict(source_path="./tmp",
                       filters=[],
                       exclude_failing_queries=True,
                       stratify_offsets=False,
                       stratify_failing=False)

    # Create dataset
    graphs = load_graphs_from_disk(**loader_args)

    features, _ = preprocess_dataset_baseline(dataset=GraphDataset(graphs),
                                              feature_statistics=feature_statistics,
                                              featurization=Featurization.STANDARD,
                                              target=LABEL.PLAT)

    pred_runtimes = model.predict(features, num_iteration=model.best_iteration)

    cheapest = np.Inf
    best_graphs = []

    # Find cheapest placement
    for (runtime, graph) in zip(pred_runtimes, graphs):
        if runtime < cheapest:
            best_graphs = [graph]
            cheapest = runtime
        elif runtime == cheapest:
            best_graphs.append(graph)

    print(f'Unique labels: {len(np.unique(pred_runtimes))}, Length of best graphs: {len(best_graphs)}')
    best_graph = random.choice(list(reversed(best_graphs)))

    #print(f'Cheapest graph is {best_graph["graph_name"]} with costs {cheapest})')

    # clean candidates: remove labels and data characteristics to prepare them for a real execution
    optimized_query, _, _, _ = parse_query(best_graph["graph_path"], clean=True, metric=args.metric)

    report = dict(query=query_name,
                  initial_true_value=initial_value,
                  est_best_value=cheapest,
                  est_speedup=initial_value/cheapest)

    write_graphs_to_disk([optimized_query], output_path, query_name + "-optimized", candidate=False)
    shutil.rmtree("./tmp")

    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', default=None, required=True)
    parser.add_argument('--output_path', default="./new_optimized_baseline_queries")
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--training_path', required=True)
    parser.add_argument('--num', default=100)
    parser.add_argument('--direction', choices=['minimize', 'maximize'], default="minimize")
    parser.add_argument('--metric', choices=["e2e-mean", "proc-mean"], required=True)

    # do not use max/min-value but percentile to avoid using prediction outliers
    args = parser.parse_args()

    results = []
    counter = dict()

    paths = dict(stats=args.training_path + "statistics.json")
    feature_statistics = load_or_create_feat_statistics(paths)

    for file in tqdm(os.listdir(args.input_path)):
        # print(counter)
        if file.endswith(".query") and "optimized" not in file:
            query_type = determine_query_type(os.path.join(args.input_path, file))
            result = optimize_single_query(args.input_path, args.output_path, file, args, feature_statistics)
            print(result)
            if result:
                result["query_type"] = query_type
                results.append(result)
                # counter[query_type] += 1
    print(len(os.listdir(args.input_path)))
    pd.DataFrame.from_dict(results).to_csv(args.output_path + '/new_prediction_baseline_results.csv')
    shutil.rmtree("./tmp", ignore_errors=True)
    sys.exit()
