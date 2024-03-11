import argparse
import os
import sys
import warnings
from typing import List

import lightgbm as lgb
import networkx as nx
import numpy as np
import pygraphviz as pgv
import torch as th
from pygraphviz import DotError
from tqdm import tqdm

from learning.constants import LABEL
from learning.dataset.dataset import GraphDataset
from learning.dataset.dataset_creation import load_dataset, encode, load_graphs_from_disk
from learning.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from learning.training.metrics import QError, Accuracy, Metric

warnings.filterwarnings('ignore', category=UserWarning, message='TypedStorage is deprecated')


class Featurization:
    STANDARD = {
            "host": ["num"],
            "spout": ["num"],
            "filter": ["num", "realSelectivity"],
            "aggregation": ["num", "realSelectivity"],
            "windowedAggregation": ["num", "realSelectivity"],
            "join": ["num", "realSelectivity"],
            "sink": ["num"]
        }

    NO_SELECTIVITIES = {
        "host": ["num"],
        "spout": ["num", "confEventRate"],
        "filter": ["num"],
        "aggregation": ["num"],
        "windowedAggregation": ["num"],
        "join": ["num"],
        "sink": ["num"]
    }


def load_or_create_baseline_dataset(path: str,
                                    prefix: str,
                                    target: str,
                                    dataset: GraphDataset,
                                    feature_statistics: dict,
                                    featurization: dict):
    if not os.path.exists(path):
        os.makedirs(path)

    if featurization == Featurization.STANDARD:
        feature_path = f'{path}/{prefix}_features_standard.npy'
    else:
        feature_path = f'{path}/{prefix}_features_no_sel.npy'
    labels_path = f'{path}/{prefix}_labels_{target}.npy'

    if not os.path.exists(feature_path) or not os.path.exists(labels_path):
        print(f'No baseline dataset found at {path} for {prefix} and target {target}')
        features, labels = preprocess_dataset_baseline(dataset=dataset,
                                                       feature_statistics=feature_statistics,
                                                       target=target,
                                                       featurization=featurization)

        np.save(feature_path, features, allow_pickle=True)
        np.save(labels_path, labels, allow_pickle=True)

    else:
        print(f'Loading baseline datasets from {path} for {prefix} and target {target}')
        features = np.load(feature_path)
        labels = np.load(labels_path)

    return features, labels


def train_baseline(paths: dict,
                   target: str,
                   dataset: str,
                   metrics: List[Metric],
                   objective:str,
                   featurization: dict):

    dataset_args = dict(dataset_path=paths["dataset"], name=dataset)
    train_dataset, val_dataset, test_dataset = load_dataset(**dataset_args)

    feature_statistics = load_or_create_feat_statistics(paths)
    args = dict(target=target,
                feature_statistics=feature_statistics,
                path=paths["model_path"],
                featurization=featurization)

    train_features, train_labels = load_or_create_baseline_dataset(dataset=train_dataset,
                                                                   prefix="train",
                                                                   **args)

    val_features, val_labels = load_or_create_baseline_dataset(dataset=val_dataset,
                                                               prefix="val",
                                                               **args)
    test_features, test_labels = load_or_create_baseline_dataset(dataset=test_dataset,
                                                                 prefix="test",
                                                                 **args)

    print(train_features.shape, train_labels.shape)
    train_data = lgb.Dataset(train_features, label=train_labels)
    val_data = lgb.Dataset(val_features, label=val_labels, reference=train_data)

    bst = lgb.train(params=dict(metric='mse',
                                objective=objective,
                                verbose=-1,
                                early_stopping_rounds=100),
                    train_set=train_data,
                    num_boost_round=1000,
                    valid_sets=[val_data])

    pred_runtimes = bst.predict(test_features, num_iteration=bst.best_iteration)

    test_stats = dict()
    for metric in metrics:
        metric.evaluate(metrics_dict=test_stats,
                        model=None,
                        labels=test_labels,
                        preds=pred_runtimes,
                        probs=None)
    # Store model to disk
    bst.save_model(f'baseline_{target}.txt')

    # save_csv([test_stats], test_path)
    return


def extract_flat_vector(graph: nx.DiGraph,
                        featurization: dict,
                        feature_statistics: dict):

    # Initialize feature counts and sums
    feature_sums = {category: {feature: 0.0 for feature in feature_list} for category, feature_list in featurization.items()}

    # Iterate over nodes in the graph
    for node, node_data in graph.nodes(data=True):
        operator_type = node_data["operatorType"]
        if operator_type in featurization.keys():
            for feature_name in featurization[operator_type]:
                if feature_name == "num":
                    feature_sums[operator_type][feature_name] += 1
                else:
                    #if feature_name == "realSelectivity":
                    #    node_data[feature_name] = max(0.0, float(node_data[feature_name]))
                    feature_sums[operator_type][feature_name] += float(node_data[feature_name])
        else:
            raise RuntimeError(f'Operator of type {operator_type} not supported')

    # Average the features
    for operator_type in feature_sums.keys():
        feature_categories = feature_sums[operator_type]
        num_operators = feature_categories["num"]
        for feature in feature_categories:
            if feature != "num":
                if num_operators != 0.0:
                    feature_categories[feature] /= num_operators
        feature_sums[operator_type] = feature_categories

    # Encode the features
    for operator_type in feature_sums.keys():
        feature_categories = feature_sums[operator_type]
        num_operators = feature_categories.pop("num")
        encoded_features = encode(feature_list=feature_categories,
                                  feature_statistics=feature_statistics)
        feature_categories = th.cat((th.tensor([num_operators]), encoded_features))
        feature_sums[operator_type] = feature_categories

    # Concat the features to a flat vector
    tensor_list = []
    for operator_type in feature_sums.keys():
        tensor_list.append(feature_sums[operator_type])

    flat_vector = th.cat(tensor_list, dim=0)
    if featurization == Featurization.STANDARD:
        assert flat_vector.size()[0] == 11, "missing feature!"
    if featurization == Featurization.NO_SELECTIVITIES:
        assert flat_vector.size()[0] == 8, "missing feature!"
    flat_vector = flat_vector.detach().cpu().numpy()
    return flat_vector


def preprocess_dataset_baseline(dataset: GraphDataset,
                                feature_statistics: dict,
                                target: str,
                                featurization: dict) -> [np.array, np.array]:
    dataset_list, label_list = [], []
    for g in tqdm(dataset, desc="Pre-encode queries"):
        try:
            graph = nx.DiGraph(pgv.AGraph(g["graph_path"]))  # .to_directed()
            features = extract_flat_vector(graph=graph,
                                           featurization=featurization,
                                           feature_statistics=feature_statistics)
            label = float(g["label"][target])
            dataset_list.append(features)
            label_list.append(label)
        except DotError:
            raise UserWarning("Could not read file correctly: ", g["graph_path"])

    features = np.vstack(dataset_list)
    labels = np.array(label_list)
    return features, labels


def predict_baseline(paths: dict,
                     test_data: str,
                     target: str,
                     metrics: List[Metric],
                     featurization: dict):

    model = lgb.Booster(model_file=f'{paths["model_path"]}/baseline_{target}.txt')
    loader_args = dict(source_path=test_data, filters=[])

    if target == "failing":
        # Using a different feature set for query success
        loader_args.update(dict(exclude_failing_queries=False, stratify_offsets=False, stratify_failing=False))

    else:
        loader_args.update(dict(exclude_failing_queries=True, stratify_offsets=False, stratify_failing=False))

    # Create dataset
    graphs = load_graphs_from_disk(**loader_args)

    feature_statistics = load_or_create_feat_statistics(paths)

    features, labels = preprocess_dataset_baseline(dataset=GraphDataset(graphs),
                                                   feature_statistics=feature_statistics,
                                                   featurization=featurization,
                                                   target=target)

    pred_runtimes = model.predict(features, num_iteration=model.best_iteration)
    # print(pred_runtimes)
    test_stats = dict()
    for metric in metrics:
        metric.evaluate(metrics_dict=test_stats,
                        model=None,
                        labels=labels,
                        preds=pred_runtimes,
                        probs=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_data', default=None, required=True)
    parser.add_argument('--test_data', default=None, required=False)
    parser.add_argument('--targets', default=None, required=True, choices=[LABEL.ELAT, LABEL.PLAT, LABEL.TPT, LABEL.FAIL, LABEL.BACKPRESSURE], nargs='+')
    parser.add_argument('--mode', choices=["train", "predict"], required=True)
    args = parser.parse_args()

    paths = dict(model_path="experimental_data/09_baseline_models",
                 graphs=args.training_data,
                 stats=args.training_data + "/statistics.json",
                 dataset=args.training_data + "/datasets/")

    for target in args.targets:
        print(f'Target {target}')
        if target in [LABEL.TPT, LABEL.ELAT, LABEL.PLAT]:
            dataset = "non_fail"
            metrics = [QError(percentile=50), QError(percentile=95)]
            objective = "regression"
            featurization = Featurization.STANDARD

        elif target in [LABEL.FAIL]:
            dataset = "full_failing_strat"
            metrics = [Accuracy()]
            objective = "binary"
            featurization = Featurization.NO_SELECTIVITIES

        elif target in [LABEL.BACKPRESSURE]:
            dataset = "non_fail_bp_strat"
            objective = "binary"
            metrics = [Accuracy()]
            featurization = Featurization.STANDARD

        else:
            raise RuntimeError("Target not defined")

        if args.mode == "train":
            train_baseline(paths=paths,
                           target=target,
                           dataset=dataset,
                           metrics=metrics,
                           objective=objective,
                           featurization=featurization)

        elif args.mode == "predict":
            predict_baseline(paths=paths,
                             test_data=args.test_data,
                             target=target,
                             metrics=metrics,
                             featurization=featurization)
    sys.exit()
