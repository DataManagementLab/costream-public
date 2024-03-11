import os
import random
import shutil
import sys

import networkx as nx
import pandas as pd
from networkx.drawing.nx_agraph import write_dot

import argparse
import pygraphviz as pgv

from learning.utils.utils import determine_query_type
from predict import predict


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


def optimize_single_query(input_path, output_path, query_file, args, query_type):
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
    write_graphs_to_disk(enumerations, "./tmp", query_name, candidate=True)
    predictions_df = pd.DataFrame()

    for i, latency_model in enumerate(args.latency_model_ids):
        latency_predictions = predict(test_path="./tmp", model_id=latency_model,
                              config_path=args.latency_config_path, training_path=args.training_path)
        tmp_df = pd.DataFrame().from_dict(latency_predictions).drop(["real_value", "qerror"], axis=1)
        tmp_df = tmp_df.rename(index=str, columns={'prediction': 'latency_prediction_' + str(i)})
        if predictions_df.empty:
            predictions_df = tmp_df
        else:
            predictions_df = pd.merge(predictions_df, tmp_df, on=["name"])

    for i, failing_model in enumerate(args.failing_model_ids):
        failing_predictions = predict(test_path="./tmp", model_id=failing_model,
                                        config_path=args.failing_config_path, training_path=args.training_path)

        tmp_df = pd.DataFrame().from_dict(failing_predictions).drop(["real_value", "accuracy"], axis=1)
        tmp_df = tmp_df.rename(index=str, columns={'prediction': 'failing_prediction_' + str(i)})

        if not predictions_df.empty:
            predictions_df = pd.merge(predictions_df, tmp_df, on=["name"])

    offset_predictions = predict(test_path="./tmp",
                                 model_id=args.offset_model_id,
                                 config_path=args.offset_config_path,
                                 training_path=args.training_path)

    offsets_df = pd.DataFrame().from_dict(offset_predictions).drop(["real_value", "accuracy"], axis=1)
    offsets_df = offsets_df.rename(index=str, columns={'prediction': 'offset_prediction'})
    predictions_df = pd.merge(predictions_df, offsets_df, on=["name"])

    for col in list(predictions_df.columns):
        if str(col).startswith("failing") or str(col).startswith("offset"):
            predictions_df[col] = round(predictions_df[col])

    # Filter out candidates that are either failing or show backpressure
    for col in list(predictions_df.columns):
        if str(col).startswith("failing"):
            predictions_df = predictions_df[predictions_df[col] == 0]
        if str(col).startswith("offset"):
            predictions_df = predictions_df[predictions_df[col] == 0]

    # removing predictions with diff > 1000
    predictions_df["diff_1"] = abs(predictions_df.latency_prediction_0 - predictions_df.latency_prediction_1)
    predictions_df["diff_2"] = abs(predictions_df.latency_prediction_0 - predictions_df.latency_prediction_2)
    predictions_df["diff_3"] = abs(predictions_df.latency_prediction_1 - predictions_df.latency_prediction_2)

    predictions_df = predictions_df[predictions_df["diff_1"] <= 2000]
    predictions_df = predictions_df[predictions_df["diff_2"] <= 2000]
    predictions_df = predictions_df[predictions_df["diff_3"] <= 2000]

    predictions_df["avg"] = predictions_df.latency_prediction_0 \
                             + predictions_df.latency_prediction_1 \
                             + predictions_df.latency_prediction_2 / 3.0

    # identify optimal prediction out of remaining candidates, looking at percentile of candidates
    if args.direction == "minimize":
        percentile = predictions_df.avg.quantile(args.percentile, interpolation="higher")
    elif args.direction == "maximize":
        percentile = predictions_df.avg.quantile(1 - args.percentile, interpolation="higher")
    else:
        raise RuntimeError("Optimization direction not supported")

    optimum = predictions_df[predictions_df.avg == percentile]

    # Could an optimum be identified?
    if len(optimum) == 0:
        print("No optimum was identified for query: " + query_name)
        shutil.rmtree("./tmp")
        return None

    # sometimes, same predictions can occur, so remove doubled values in the optimum
    if len(optimum) >= 1:
        optimum = optimum[0:1]

    if optimum["avg"].values[0] >= initial_value:
        print("Query cannot be optimized further: " + query_name)
        shutil.rmtree("./tmp")
        return None

    # clean candidates: remove labels and data characteristics to prepare them for a real execution
    optimized_query, _, _, _ = parse_query(os.path.join("./tmp", optimum["name"].values[0] + ".query"),
                                           clean=True, metric=args.metric)
    # Delete temporary directory
    shutil.rmtree("./tmp")

    report = dict(query=query_name,
                  initial_true_value=initial_value,
                  est_best_value=float(optimum["avg"].values[0]),
                  est_speedup=initial_value / float(optimum["avg"].values[0]),
                  failing=float(round(optimum["avg"].values[0])))

    write_graphs_to_disk([optimized_query], output_path, query_name + "-optimized", candidate=False)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', default=None, required=True)
    parser.add_argument('--output_path', default="./optimized_queries")
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--latency_config_path', required=True)
    parser.add_argument('--training_path', required=True)
    parser.add_argument('--latency_model_ids',  nargs='+', required=True, type=str)
    parser.add_argument('--failing_model_ids',  nargs='+', required=True, type=str)
    parser.add_argument('--failing_config_path', required=True)
    parser.add_argument('--offset_model_id', required=True)
    parser.add_argument('--offset_config_path', required=True)
    parser.add_argument('--num', default=25)
    parser.add_argument('--direction', choices=['minimize', 'maximize'], default="minimize")
    parser.add_argument('--metric', choices=["e2e-mean", "proc-mean"], required=True)

    # do not use max/min-value but percentile to avoid using prediction outliers
    parser.add_argument('--percentile', default=0.1)
    args = parser.parse_args()

    results = []
    counter = dict()
    for file in os.listdir(args.input_path):
        print(counter)
        if file.endswith(".query") and "optimized" not in file:
            query_type = determine_query_type(os.path.join(args.input_path, file))
            if query_type not in counter.keys():
                counter[query_type] = 0
            if counter[query_type] < 50:
                result = optimize_single_query(args.input_path, args.output_path, file, args, query_type)
                if result:
                    result["query_type"] = query_type
                    results.append(result)
                    counter[query_type] += 1
    pd.DataFrame.from_dict(results).to_csv(args.output_path + '/prediction_results.csv')
    shutil.rmtree("./tmp", ignore_errors=True)
    sys.exit()
