import itertools
import sys

import pygraphviz as pgv
import os
import argparse
from tqdm import tqdm

FONTSIZE = 15

EXCLUDE_LIST = ["id", "label", "operatorType", "style", "grouping", "host",
                "duration", "query", "_id", "component", "inputCounter", "outputCounter"]
DATA_CHARACTERISTICS = ["tupleWidthIn", "tupleWidthOut", "realSelectivity", "inputRate", "outputRate"]
HARDWARE_FEATURES = ["cpu", "ram", "bandwidth", "ramswap", "latency", "category"]
LABELS = ["throughput", "proc-latency", "e2e-latency", "counter", "offset", "throughput-mean", "e2e-mean", "proc-mean"]


def get_attr(node):
    """Yield attributes from a node"""
    keys = sorted(node.attr.keys())
    for k in keys:
        v = node.attr.get(k)
        if v != '' and k not in EXCLUDE_LIST:
            try:
                v = int(v)
            except ValueError:
                try:
                    v = round(float(v), 2)
                except ValueError:
                    pass
            v = str(v)
            if v.endswith(".0"):
                v = v[:-2]
            yield k, v


def generate_additional_labels(node):
    """Generate additional labels out of the node attributes"""
    label = str()

    # pushing data characteristics to the very end
    for (k, v) in get_attr(node):
        combined = k + ": " + v
        if k not in DATA_CHARACTERISTICS + LABELS + HARDWARE_FEATURES:
            combined = "<FONT COLOR=\"darkblue\" FACE = \"courier\" >" + combined + "</FONT>"
            combined = combined + "<br />"
            label = label + combined

    for (k, v) in get_attr(node):
        combined = k + ": " + v
        if k in DATA_CHARACTERISTICS:
            combined = "<FONT COLOR=\"darkred\" FACE = \"courier\" >" + combined + "</FONT>"
            combined = combined + "<br />"
            label = label + combined

        if k in LABELS:
            combined = "<FONT COLOR=\"orange\" FACE = \"courier\" >" + combined + "</FONT>"
            combined = combined + "<br />"
            label = label + combined

        if k in HARDWARE_FEATURES:
            combined = "<FONT COLOR=\"darkgreen\" FACE = \"courier\" >" + combined + "</FONT>"
            combined = combined + "<br />"
            label = label + combined

    return "<" + label + ">"


def read_from_dir(path):
    """Read queries from disk"""
    assert os.path.exists(path)
    queries = []
    for n in os.listdir(path):
        if n.endswith(".query") or n.endswith(".graph"):
            queries.append(n)
    print(len(queries), " graphs found in directory" + path)
    return queries


def main(input_dir: str, output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    queries = read_from_dir(input_dir)
    # iterate over graphs and add formatting
    for name in tqdm(queries):

        target = os.path.join(input_dir, name)
        graph = pgv.AGraph(target)

        graph.graph_attr.update({"label": name, "fontsize": FONTSIZE})

        # subgraph for hosts
        hosts = graph.add_subgraph()
        hosts.graph_attr.update(rank='max')

        # subgraph for spouts
        spouts = graph.add_subgraph()
        spouts.graph_attr.update(rank='min')

        # collect groupings in dict
        groups = dict()

        # iterate over nodes
        for node in graph.nodes():
            inventory = graph.get_node(node).attr

            # add potential grouping information
            if inventory["grouping"]:
                if inventory["grouping"] not in groups.keys():
                    groups[inventory["grouping"]] = []
                groups[inventory["grouping"]].append(node)

            # format nodes
            inventory.update({"xlabel": generate_additional_labels(node),
                              "style": "filled",
                              "fontsize": FONTSIZE})

            # format operators
            if inventory["operatorType"] != "host":
                inventory.update({"label": inventory["id"] + "\n" + inventory["operatorType"],
                                  "fillcolor": "lightblue"})
                if inventory["operatorType"] == "spout":
                    inventory.update({"fillcolor": "lightyellow"})
                    spouts.add_node(node)

            # format hosts
            else:
                inventory.update({"fillcolor": "lightgreen", "shape": "box"})
                if inventory["host"]:
                    inventory.update({"label": inventory["host"]})
                hosts.add_node(node)
            graph.get_node(node).attr = inventory

        # draw additional connections for storm internal groupings
        for group in groups.values():
            group_subgraph = graph.add_subgraph()
            for node in group:
                group_subgraph.add_node(node)
            for pair in list(itertools.combinations(group_subgraph, 2)):
                group_subgraph.add_edge(pair[0], pair[1])

            for edge in group_subgraph.edges():
                edge.attr.update({"color": "red",
                                  "dir": "both",
                                  "penwidth": "1.5",
                                  "arrowhead": "curve",
                                  "arrowtail": "curve"})

        # prettify plot
        for edge in graph.edges():
            if edge.attr.get("bandwidth") or edge.attr.get("network-usage"):
                edge.attr.update({"fontsize": FONTSIZE,
                                  "label": generate_additional_labels(edge)})
            edge.attr.update(penwidth="2.5")

            if edge[0].get_name().startswith("host") != edge[1].get_name().startswith("host"):
                edge.attr.update({"color": "orange", "penwidth": "2.5"})
            elif edge[0].get_name().startswith("host") and edge[1].get_name().startswith("host"):
                edge.attr.update(color="darkgreen")

        graph.draw(os.path.join(output_dir, name.split(".")[0] + ".png"), prog="dot")
    print("All graphs visualized successfully!")
    sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Drawing query graphs by reading in a directory of DOT files'
                                                 ' and writing png-files to an output directory')
    parser.add_argument("--input", required=True, type=str, help="Path to graph filed")
    parser.add_argument("--output", required=True, type=str, help="Output directory")
    args = parser.parse_args()
    main(args.input, args.output)
