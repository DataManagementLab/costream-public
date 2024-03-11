import collections

import numpy as np
import torch
import torch as th
import dgl as dgl
import pygraphviz as pgv
import networkx as nx
from dgl import DGLHeteroGraph


def flatten_dict(d, parent_key='', sep='_'):
    """
    https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def recursive_to(iterable, device):
    if isinstance(iterable, (dgl.DGLGraph, DGLHeteroGraph)):
        iterable.to(device)
    if isinstance(iterable, torch.Tensor):
        iterable.data = iterable.data.to(device)
    elif isinstance(iterable, collections.abc.Mapping):
        for v in iterable.values():
            recursive_to(v, device)
    elif isinstance(iterable, (list, tuple)):
        for v in iterable:
            recursive_to(v, device)


def batch_to(batch, device, label_norm):
    graph, features, label = batch

    # normalize the labels for training
    if label_norm is not None:
        label = label_norm.transform(label.reshape(-1, 1))
        label = label.reshape(-1)
    else:
        label = np.array(label)
    label = th.from_numpy(label.astype(np.float32))

    recursive_to(label, device)
    recursive_to(features, device)
    # recursive_to(graph, device)
    graph = graph.to(device)
    return (graph, features), label


def determine_query_type(path: str):
    QUERY_TYPES = {0: "Linear", 1: "2-Way-Join", 2: "3-Way-Join"}
    joins = 0
    aggregation = False
    graph = nx.DiGraph(pgv.AGraph(path).to_directed())
    for node, data in graph.nodes(data=True):
        if data["operatorType"] == "join":
            joins += 1
        if data["operatorType"] in ["aggregation", "windowedAggregation"]:
            aggregation = True
    # Determine query name
    query_type = QUERY_TYPES[joins]
    if aggregation:
        query_type = query_type + "\nQuery\nwith Aggregation"
    else:
        query_type = query_type + "\nQuery"
    return query_type