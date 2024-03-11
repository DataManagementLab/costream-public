import os

from pygraphviz import DotError
from tqdm import tqdm
import pygraphviz as pgv
import networkx as nx
from sklearn.preprocessing import RobustScaler
import numpy as np
import json

from learning.constants import Feat, Info, LABEL


def typecast_features(feature_name, feature_values):
    """Do necessarily typecast for the given features"""
    if feature_name in [Feat.EVENT_RATE, Feat.NUM_DOUBLE, Feat.NUM_STRING,
                        Feat.NUM_INTEGER, Feat.TUPLE_W_IN, Feat.TUPLE_W_OUT,
                        Feat.SLIDING_LENGTH, Feat.WINDOW_LENGTH, Feat.CPU, Feat.RAM, Feat.LATENCY, Feat.BANDWIDTH,
                        Feat.SELECTIVITY, LABEL.ELAT, LABEL.PLAT, LABEL.TPT, LABEL.BACKPRESSURE,
                        "dummy"]:
        #try:
        feature_values = [float(i) for i in feature_values]

    elif feature_name in [Info.GROUP, Info.HOST, Info.ID, Info._ID, Info.LITERAL,
                          Info.INPUT_RATE, Info.OUTPUT_RATE, Info.COMPONENT,
                          Info.DURATION, Info.QUERY, Info.INGESTION_INTERVAL, Info.KAFKA_DURATION,
                          Info.PRODUCER_INTERVAL, Info.TOPIC, Info.INPUT_COUNTER, Info.OUTPUT_COUNTER, Info.PORT,
                          Info.RAMSWAP, Info.SCORE, Info.COUNTER, Info.CATEGORY]:
        return None

    # ToDo: Do we really need to encode the operator type here?
    elif feature_name in [Feat.INSTANCE_SIZE, Info.OP_TYPE, Feat.JOIN_CLASS, Feat.WINDOW_POLICY, Feat.WINDOW_TYPE,
                          Feat.AGG_FUNCTION, Feat.AGG_CLASS,
                          Feat.GROUP_BY_CLASS, Feat.FILTER_FUNCTION, Feat.FILTER_CLASS,
                          Info.PORT]:
        # do nothing, categorical embedding
        return feature_values
    else:
        raise Exception("Feature " + feature_name + " not in features")
    return feature_values


def compute_feature_statistic(feature_values):
    """
    :param feature_values:  A list of values (categorical or numerical) from a specific feature
    where to extract statistics from
    :return: dictionary of feature statistics / embeddings
    """
    if all([isinstance(v, int) or isinstance(v, float) or v is None for v in feature_values]):
        scaler = RobustScaler()
        np_values = np.array(feature_values, dtype=np.float64).reshape(-1, 1)
        scaler.fit(np_values)
        feature_statistic = dict(max=float(np_values.max()),
                                 scale=scaler.scale_.item(),
                                 center=scaler.center_.item(),
                                 type=str("numeric"))
    else:
        unique_values = set(feature_values)
        feature_statistic = dict(value_dict={v: id for id, v in enumerate(unique_values)},
                                 no_vals=len(unique_values),
                                 type=str("categorical"))
    return feature_statistic


def load_or_create_feat_statistics(data_paths):
    """ Writes the statistics file to disk
    :param data_paths: Dictionary that contains related paths
    :return: none
    """
    if os.path.exists(data_paths["stats"]):
        print("Statistics exists at: ", data_paths["stats"])
        with open(data_paths["stats"]) as stat_file:
            statistics_dict = json.load(stat_file)

    else:
        print("Creating statistics at: " + data_paths["stats"])

        # Create a plain list of all operators in the training data
        plain_operator_list = []
        for g in tqdm(os.listdir(data_paths["graphs"]), desc="Gather feature statistics"):
            if os.path.isfile(os.path.join(data_paths["graphs"], g)) and g.endswith(".query"):
                try:
                    graph = nx.Graph(pgv.AGraph(os.path.join(data_paths["graphs"], g)))
                    graph_nodes = list(graph.nodes(data=True))
                    for _, operator in graph_nodes:
                        plain_operator_list.append(operator)
                except DotError:
                    print("Error while reading: " + g)

        # Create a feature dictionary that contains each feature with its corresponding list of values
        feature_dict = dict()
        for operator in tqdm(plain_operator_list, desc="Collecting features from all operators"):
            for key in operator.keys():
                feature_list = feature_dict.get(key)
                if not feature_list:
                    feature_list = []
                feature = operator.get(key)
                feature_list.append(feature)
                feature_dict[key] = feature_list

        feature_dict["dummy"] = [0]
        # preprocess all features
        statistics_dict = dict()
        for feature_name, feature_values in tqdm(feature_dict.items(), desc="Extract statistics from feature dictionary"):
            feature_values = typecast_features(feature_name, feature_values)
            if feature_values:
                statistics_dict[feature_name] = compute_feature_statistic(feature_values)

        # save as json
        with open(data_paths["stats"], 'w') as outfile:
            json.dump(statistics_dict, outfile, sort_keys=True)

    # Do encoding of queries already here to do it only once - then generate DataLoader with transformed queries
    for k, v in statistics_dict.items():
        if v.get('type') == str("numeric"):
            scaler = RobustScaler()
            scaler.center_ = v['center']
            scaler.scale_ = v['scale']
            statistics_dict[k]['scaler'] = scaler

    return statistics_dict


