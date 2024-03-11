import os

import dgl
import networkx as nx
import torch as th
import wandb
from networkx import DiGraph
from pygraphviz import DotError
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
from tqdm import tqdm
import numpy as np
import pygraphviz as pgv
import pandas as pd

from learning.constants import Info, LABEL, Operators, OPERATOR, QUERY, Feat
from learning.dataset.dataset import GraphDataset
from learning.training.zero_shot_model import MyPassDirection


def load_graphs_from_disk(source_path: str, exclude_failing_queries: bool, stratify_offsets: bool, stratify_failing: bool,
                          filters: list, query_files: list = []):
    """Loading graph paths from the source_path and labels from the label-file into a common dictionary
    :param: source_path: Path where to find all graphs
    :param: label_path: Path where labels are stored
    :return: List of dicts that contain each the graph to a path"""

    labels = extract_labels_from_directory(queries_path=source_path,
                                           exclude_failing_queries=exclude_failing_queries,
                                           offset_to_boolean=True,
                                           stratify_offsets=stratify_offsets,
                                           stratify_failing=stratify_failing,
                                           filters=filters,
                                           query_files=query_files)
    labels = labels.to_dict('index')
    queries = []
    if not query_files:
        query_files = [f for f in os.listdir(source_path) if f.endswith(".query")]
    for query_path in tqdm(query_files, desc="Load graphs"):
        query_name = query_path.split(".")[0]
        if query_name in labels.keys():
            query_label = labels[query_name]
            query_path = os.path.join(source_path, query_path)
            queries.append(dict(graph_path=query_path, graph_name=query_name, label=query_label))
    if not queries:
        raise Exception("No queries found in path: ", source_path)
    return queries


def obtain_label_normalizer(train_data: GraphDataset, val_data: GraphDataset):
    label_vector = []
    for sample in train_data + val_data:
        label_vector.append(float(sample["label"][wandb.config["target"]]))
    label_vector = np.array(label_vector)
    print("Value range of label " + wandb.config["target"] + " is: " + str(min(label_vector)) + " to " + str(max(label_vector)))

    if wandb.config["loss_class_name"] == 'MSELoss':
        log_transformer = preprocessing.FunctionTransformer(np.log1p, _inv_log1p, validate=True)
        scale_transformer = preprocessing.MinMaxScaler()
        pipeline = Pipeline([("log", log_transformer), ("scale", scale_transformer)])
        pipeline.fit(label_vector.reshape(-1, 1))
    elif wandb.config["loss_class_name"] == 'QLoss':
        scale_transformer = preprocessing.MinMaxScaler(feature_range=(1, 1000))
        pipeline = Pipeline([("scale", scale_transformer)])
        pipeline.fit(label_vector.reshape(-1, 1))
    else:
        pipeline = None
    return pipeline


def _inv_log1p(x):
    return np.exp(x) - 1


def create_dataset(dataset_path: str, graph_path: str, exclude_failing_queries: bool,
                   stratify_offsets: bool, stratify_failing: bool, name: str,
                   filters: list = None, limit_size: int = None):
    training_frac, validation_frac, test_frac = 0.8, 0.1, 0.1
    npy_paths = [name+"_train.npy", name+"_validate.npy", name+"_test.npy"]

    # Check if paths exist and abort to not overwrite existing datasets
    for path in npy_paths:
        if os.path.exists(path):
            return

    assert training_frac + validation_frac + test_frac == 1
    os.makedirs(dataset_path, exist_ok=True)

    # train and validation graphs
    graphs = load_graphs_from_disk(source_path=graph_path,
                                   exclude_failing_queries=exclude_failing_queries,
                                   stratify_offsets=stratify_offsets,
                                   stratify_failing=stratify_failing,
                                   filters=filters)

    if limit_size is not None:
        graphs = graphs[0:limit_size]

    training_data_num = int(training_frac * len(graphs))
    validation_data_num = int(validation_frac * len(graphs))
    shuffled_graph_indxs = list(range(len(graphs)))
    np.random.shuffle(shuffled_graph_indxs)

    train_dataset = GraphDataset([graphs[i] for i in shuffled_graph_indxs[:training_data_num]])
    val_dataset = GraphDataset(
        [graphs[i] for i in shuffled_graph_indxs[training_data_num: training_data_num + validation_data_num]])
    test_dataset = GraphDataset([graphs[i] for i in shuffled_graph_indxs[training_data_num + validation_data_num:]])

    np.save(dataset_path + "/" + npy_paths[0], train_dataset, allow_pickle=True)
    np.save(dataset_path + "/" + npy_paths[1], val_dataset, allow_pickle=True)
    np.save(dataset_path + "/" + npy_paths[2], test_dataset, allow_pickle=True)

    print(f"\nDataset \033[1m{name}\033[0m created with total size {len(graphs)} and distribution: "
          f"Training: {len(train_dataset)}, "
          f"Validation: {len(val_dataset)}, "
          f"Test {len(test_dataset)})")


def load_dataset(dataset_path: str, name: str):
    print(f"Loading dataset with name {name} from directory: {dataset_path}")
    npy_paths = [name+"_train.npy", name+"_validate.npy", name+"_test.npy"]

    if any(path in npy_paths for path in os.listdir(dataset_path)):
        train_dataset = GraphDataset(np.load(dataset_path + "/" + npy_paths[0], allow_pickle=True))
        val_dataset = GraphDataset(np.load(dataset_path + "/" + npy_paths[1], allow_pickle=True))
        test_dataset = GraphDataset(np.load(dataset_path + "/" + npy_paths[2], allow_pickle=True))
    else:
        raise Exception(f"Datasets not found at {dataset_path} with name {name}")

    return train_dataset, val_dataset, test_dataset


def extract_labels_from_directory(queries_path: str, exclude_failing_queries: bool,
                                  offset_to_boolean: bool, stratify_offsets: bool,
                                  stratify_failing: bool, filters: list, query_files: list = []):
    """
    Reading through a directory and grabbing out the labels out of the graphs
    """

    if not (os.path.exists(queries_path)):
        raise Exception(queries_path + " not found on disk")

    label_dict = dict()
    if not query_files:
        query_files = [q for q in os.listdir(queries_path) if q.endswith(".query")]

    if not query_files:
        raise Exception(queries_path + " is empty!")

    for query_file in tqdm(query_files, desc="Reading labels from directory: " + queries_path):
        query_path = os.path.join(queries_path, query_file)
        assert os.path.isfile(query_path)
        query_name = query_file.split(".")[0]

        joins = 0
        summarized_offset = 0

        hw_props = dict()
        for hw_prop in [Feat.CPU, Feat.RAM, Feat.LATENCY, Feat.BANDWIDTH]:
            hw_props[hw_prop + "-min"], hw_props[hw_prop + "-max"] = np.inf, -np.inf

        graph = nx.DiGraph(pgv.AGraph(query_path))

        for node_key, node_data in graph.nodes(data=True):
            if node_data[OPERATOR.TYPE] == Operators.SINK:
                label_dict[query_name] = node_data
            elif node_data[OPERATOR.TYPE] == Operators.JOIN:
                joins += 1
            elif node_data[OPERATOR.TYPE] == Operators.SPOUT:
                summarized_offset += int(node_data[LABEL.BACKPRESSURE])
            elif node_data[OPERATOR.TYPE] == "host":
                for hw_prop in [Feat.CPU, Feat.RAM, Feat.LATENCY, Feat.BANDWIDTH]:
                    hw_props[hw_prop + "-max"] = max(hw_props[hw_prop + "-max"], int(float(node_data[hw_prop])))
                    hw_props[hw_prop + "-min"] = min(hw_props[hw_prop + "-min"], int(float(node_data[hw_prop])))

        if query_name in label_dict.keys():
            label_dict[query_name][QUERY.TYPE] = joins
            label_dict[query_name][LABEL.BACKPRESSURE] = summarized_offset
            label_dict[query_name].update(hw_props)

    label_dataframe = pd.DataFrame.from_dict(label_dict, orient='index')
    label_dataframe = label_dataframe.replace('null', np.nan)
    label_dataframe = label_dataframe.astype(dtype={LABEL.ELAT: np.float32,
                                                    LABEL.PLAT: np.float64,
                                                    LABEL.TPT: np.float64,
                                                    LABEL.BACKPRESSURE: np.float64,
                                                    LABEL.COUNTER: np.float64,
                                                    QUERY.TYPE: int})

    if exclude_failing_queries:
        label_dataframe = label_dataframe[label_dataframe[LABEL.TPT] != -1]
    else:
        label_dataframe[LABEL.FAIL] = label_dataframe[LABEL.TPT].apply(lambda x: True if x == -1 else False)
        if stratify_failing:
            label_dataframe = stratify_dataframe(label_dataframe, LABEL.FAIL)

    # Sometimes there are negative offsets in the data
    label_dataframe[LABEL.BACKPRESSURE] = label_dataframe[LABEL.BACKPRESSURE].clip(lower=0)

    # Turning offset to classification problem
    if offset_to_boolean:
        label_dataframe[LABEL.BACKPRESSURE] = label_dataframe[LABEL.BACKPRESSURE].apply(lambda x: True if x > 0 else False)
        # doing stratification here to equalize classes in datasets - this reduces and distorts the dataset!
        if stratify_offsets:
            label_dataframe = stratify_dataframe(label_dataframe, LABEL.BACKPRESSURE)

    if filters:
        for filter in filters:
            if "min_inclusive" in filter.keys():
                label_dataframe = label_dataframe[label_dataframe[filter["feature"]+"-min"]>= int(filter["min_inclusive"])]

            if "max_inclusive" in filter.keys():
                label_dataframe = label_dataframe[label_dataframe[filter["feature"]+"-max"] <= int(filter["max_inclusive"])]

    if len(label_dataframe) == 0:
        raise RuntimeError("Labels are empty")
    return label_dataframe


def stratify_dataframe(dataframe: pd.DataFrame, column: str):
    num_of_less_frequent = dataframe[column].value_counts().min()
    max_frequent_class = dataframe[column].value_counts().idxmax()
    num_of_max_frequent = dataframe[column].value_counts().max()
    diff = num_of_max_frequent - num_of_less_frequent
    entries_to_subtract = dataframe.loc[((dataframe[column] == max_frequent_class))][0:diff]
    dataframe = dataframe.drop(entries_to_subtract.index)
    return dataframe


def encode(feature_list, feature_statistics):
    """Encoding a list of features based on the previously computed feature_statistics
    :param: feature_list: A list of features to encode
    :param: feature_statistics: Dictionary that contains feature statistics
    :return: A tensor of encoded features"""

    copied_features = dict(feature_list)
    encoded_feature_list = dict()
    # Encode remaining features
    for feature in copied_features.keys():
        value = copied_features[feature]
        if feature == "dummy":
            value = 0
        if feature_statistics[feature].get('type') == str("numeric"):
            enc_value = feature_statistics[feature]['scaler'].transform(np.array([[value]])).item()
        elif feature_statistics[feature].get('type') == str("categorical"):
            value_dict = feature_statistics[feature]['value_dict']
            enc_value = value_dict[str(value)]
        else:
            raise NotImplementedError
        encoded_feature_list[feature] = th.tensor(enc_value)

    # Sort features as they are not always given in the same order
    encoded_feature_list = [encoded_feature_list[k].clone().detach() for k in sorted(encoded_feature_list.keys())]
    return th.tensor(encoded_feature_list)


def add_indexes(graph):
    """ add indexes to each operator, counting for every OperatorType and starting from 0.
    This is needed to distinguish the operators in the heterograph"""
    operators_by_type, operator_indexes_by_type = dict(), dict()
    for node_key, node_data in graph.nodes(data=True):
        operator_type = node_data["operatorType"]
        if operator_type not in operators_by_type.keys():
            operators_by_type[operator_type] = []
            operator_indexes_by_type[operator_type] = 0
        node_data["index"] = operator_indexes_by_type[operator_type]
        operator_indexes_by_type[operator_type] += 1
        operators_by_type[operator_type].append(node_data)


def add_edge(data_dict, source, edge_type, target):
    edge = (source["operatorType"], edge_type, target["operatorType"])
    if edge not in data_dict.keys():
        data_dict[edge] = ([], [])
    sources, targets = data_dict[edge]
    sources.append(source["index"])
    targets.append(target["index"])
    data_dict[edge] = (sources, targets)


def get_data_dict(graph, config):
    """obtaining data_dict for heterograph, this maintains the graph structure
    by help of previously assigned unique indexes.
    Each node may have 0, 1 or 2 children"""
    data_dict = dict()

    if config["message_passing_scheme"] == "simple":
        for e in graph.edges(data=True):
            source = graph.nodes.get(e[0])
            target = graph.nodes.get(e[1])
            # ignore host-to-host-connections
            if not (source["operatorType"] == "host" and target["operatorType"] == "host"):
                add_edge(data_dict, source, "edge", target)
                add_edge(data_dict, target, "edge", source)

    else:
        for node_key, root_data in graph.nodes(data=True):
            children = []
            for c in graph.successors(node_key):
                children.append(c)
            if len(children) > 2:
                raise RuntimeError("more than two children for operator " + root_data["id"])
            for child in children:
                child_data = graph.nodes.get(child)

                # In the original graph, we have top down, this is why root and child are swapped
                if root_data["operatorType"] != "host" and child_data["operatorType"] == "host":
                    if config["message_passing_scheme"] == "bottom-up":
                        add_edge(data_dict, child_data, "has_operator", root_data)

                    elif config["message_passing_scheme"] == "full":
                        add_edge(data_dict, child_data, "has_operator", root_data)
                        add_edge(data_dict, root_data, "is_placed_on", child_data)
                    else:
                        raise Exception("MP Scheme "+ wandb.config["message_passing_scheme"] + " not supported")

                elif root_data["operatorType"] != "host" and child_data["operatorType"] != "host":
                    add_edge(data_dict, root_data, "edge", child_data)

                elif root_data["operatorType"] == "host" and child_data["operatorType"] == "host":
                    # ignore host-to-host-connections
                    pass

                else:
                    raise Exception("Edge " + root_data["operatorType"],
                                    child_data["operatorType"] + " not supported in MP scheme")

    # convert to tensors
    for key in data_dict.keys():
        data_dict[key] = (th.tensor(data_dict[key][0]), th.tensor(data_dict[key][1]))
    return data_dict


def get_feature_dict(graph, feature_statistics, featurization):
    """ do the encoding of features, store in feature_dict"""
    feature_dict = dict()
    for node_key, node_data in graph.nodes(data=True):
        operator_type = node_data["operatorType"]
        if not operator_type in feature_dict.keys():
            feature_dict[operator_type] = []
        if operator_type == "sink":
            node_data["dummy"] = 0
        # Remove unneccessary node entries
        remove_list = []
        node_data_copy = node_data.copy()
        for feat in node_data_copy.keys():
            if feat not in featurization.ALL:
                remove_list.append(feat)
        for r in remove_list:
            node_data_copy.pop(r)
        feature_dict[operator_type].append(encode(node_data_copy, feature_statistics))
    # list of features (tensors) have to be brought into one common tensor
    for feature in feature_dict.keys():
        try:
            feature_dict[feature] = th.stack(feature_dict[feature])
            # if program fails here, the amount of features within the same operator possibly differs
        except RuntimeError:
            print("the features for: " + feature + " in "+ node_key + "cannot be stacked, there are probably features missing.")
    return feature_dict


def get_graph_data(graph: DiGraph) -> (str, int):
    """Compute the top node and the index of the top node of a given graph"""
    top_nodes = 0
    top_node, top_node_idx = None, None
    for node_key, node_data in graph.nodes(data=True):
        if node_data["operatorType"] == "sink":
            top_nodes += 1
            top_node = node_data["operatorType"]
            top_node_idx = node_data["index"]

    if top_nodes > 1:
        raise RuntimeError("Error at query: " + graph.name + " - check if it is DAG!?")

    if top_node is None or top_node_idx is None:
        raise RuntimeError("No Top node could be identified for query: " + graph.name)

    return top_node, top_node_idx


def sort_edges(canonical_etypes: list):
    start_list = []
    middle = []
    end = []
    sorted_edges = []

    # at first look for the operators --> hw-nodes edges (only full MP-scheme)
    for edge in canonical_etypes:
        if edge[1] == "is_placed_on" and edge not in sorted_edges:
            start_list.append(edge)

    # then look for hw-nodes --> operator edges (all schema)
    for edge in canonical_etypes:
        if edge[1] == "has_operator" and edge not in sorted_edges:
            start_list.append(edge)

    # finally, add edge links between nodes
    for edge in canonical_etypes:
        if edge[1] == "edge":
            if edge[0] == "spout":
                start_list.append(edge)
            elif edge[2] == "sink":
                end.append(edge)
            # put it in the middle
            else:
                middle.append(edge)

    out = start_list + middle + end
    return out


def preprocess_dataset(dataset: GraphDataset, feature_statistics: dict, target: str, config: dict, featurization):
    # preparing graph
    dataset_list = []
    for g in tqdm(dataset, desc="Pre-encode queries"):
        try:
            graph = nx.DiGraph(pgv.AGraph(g["graph_path"]))  # .to_directed()
            graph.name = g["graph_name"]
            add_indexes(graph)
            graph.label = th.tensor([float(g["label"][target])])
            graph.feature_dict = get_feature_dict(graph, feature_statistics, featurization)
            graph.data_dict = get_data_dict(graph, config)
            dataset_list.append(graph)

        except DotError:
            raise UserWarning("Could not read file correctly: ", g["graph_path"])
    return dataset_list


def collator(batch, feature_statistics, target):
    """Collating the graphs and labels of a batch to a heterograph and a label tensor
    See: https://docs.dgl.ai/en/0.6.x/generated/dgl.heterograph.html"""

    batched_labels, batch_features, batch_names, batch_top_nodes, batch_graph_data, graphs = [], [], [], [], [], []

    for graph in batch:

        batch_names.append(graph.name)
        batched_labels.append(graph.label)

        # obtaining top node and index
        top_node, top_node_index = get_graph_data(graph)
        batch_top_nodes.append(dict(top_node=top_node, top_node_index=top_node_index))

        batch_graph_data.append(graph.data_dict)
        batch_features.append(graph.feature_dict)

    # collecting all edge types from all graphs in a common set
    canonical_etypes = []
    for d in batch_graph_data:
        for edge in d.keys():
            if edge not in canonical_etypes:
                canonical_etypes.append(edge)

    # use edge set to create the pass directions
    sorted_etypes = sort_edges(canonical_etypes)
    pass_directions = []
    for edge in sorted_etypes:
        pd = MyPassDirection(model_name=edge[1], e_name=edge[1], n_src=edge[0], n_dest=edge[2])
        pass_directions.append(pd)

    # updating data dicts with remaining edge types (empty edges) that are not yet contained
    for data_dict in batch_graph_data:
        # The data_dict might be updated from previous training iterations and thus contains empty edges with types
        # that are not seen in canonical_etypes and also not reflected in the feature_dictionary. This especially
        # occurs when using small batch_sizes. For this reason, the final data_dict that is used for the heterograph
        # is created with a shallow copy.
        final_data_dict = data_dict.copy()
        for edge in canonical_etypes:
            if edge not in data_dict.keys():
               final_data_dict[edge] = ((), ())

        # create hetero_graphs
        hetero_graph = dgl.heterograph(final_data_dict)
        graphs.append(hetero_graph)

    # merging feature dicts in to a common dict:
    batched_features = dict()
    for feature in batch_features:
        for operator in feature.keys():
            if operator not in batched_features.keys():
                # ToDo: Optimize for GPU?
                batched_features[operator] = feature[operator].clone().detach()
            else:
                batched_features[operator] = th.cat([batched_features[operator], feature[operator].clone().detach()])

    # batch graphs and labels
    batched_graph = dgl.batch(graphs)
    batched_graph.pd = pass_directions
    batched_graph.names = batch_names
    batched_graph.data = batch_top_nodes
    batched_labels = th.stack(batched_labels)

    if set(list(batched_features.keys())) != set(batched_graph.ntypes):
        raise Exception("Error")

    return batched_graph, batched_features, batched_labels
