import os

import argparse
import pygraphviz as pgv
import networkx as nx
import tqdm

def analyze_data(path: str, clean:str):
    defect_queries = set()
    non_dag_queries = set()

    for graph in tqdm.tqdm([f for f in os.listdir(path) if f.endswith(".query")]):
        g = nx.DiGraph(pgv.AGraph(os.path.join(path, graph))).to_directed()
        for node in g.nodes(data=True):
            _, node_data = node
            if node_data["operatorType"] in ["filter", "join", "windowedAggregation", "aggregation"]:
                if "realSelectivity" not in node_data.keys():
                    defect_queries.add(graph)

        top_nodes = 0
        top_node, top_node_idx = None, None
        for node_key, node_data in g.nodes(data=True):
            child = []
            if not ((g.predecessors(node_key) == None) and (g.successors(node_key) == None)):
                for c in g.successors(node_key):
                    child.append(c)
                if len(child) > 2:
                    raise RuntimeError("more than one child for operator " + node_data["id"])
                if len(child) == 0:
                    top_nodes += 1
                    top_node = node_data["operatorType"]

        if top_nodes > 1:
            defect_queries.add(graph)

        if top_node is None:
            non_dag_queries.add(graph)

    defect_queries = [q.replace(".graph", "") for q in defect_queries]
    non_dag_queries = [q.replace(".graph", "") for q in non_dag_queries]
    print(str(len(defect_queries)) + " queries have missing data characteristics!")
    print(str(len(non_dag_queries)) + " queries are non DAG queries!")

    if clean:
        for graph in [f for f in os.listdir(path) if f.endswith(".query")]:
            if graph in defect_queries + non_dag_queries:
                print("removing " + graph + " from dir: " + path)
                os.remove(os.path.join(path, graph))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, required=True)
    parser.add_argument('--clean', default=False, required=False)
    args = parser.parse_args()
    analyze_data(args.path, args.clean)

