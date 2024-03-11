import functools
import os
from operator import itemgetter

import numpy as np
import pandas as pd
import torch as th
import time
import wandb
from tqdm import tqdm

from torch import optim
from torch.utils.data import DataLoader

from learning.constants import LABEL
from learning.dataset.dataset import GraphDataset
from learning.dataset.dataset_creation import collator, preprocess_dataset
from learning.training.checkpoint import load_checkpoint, save_checkpoint, save_csv
from learning.training.metrics import RMSE, QError, Accuracy, F1Score, Recall
from learning.training.zero_shot_model import ZeroShotModel
from learning.utils.utils import batch_to


def prepare_model(config, target, feature_statistics, model_dir, model_name, model_state, device, label_norm, featurization):
    """Prepares a model configuration. Either load them from disk or create a new one."""

    # general parameters for fc-out-model
    fc_out_kwargs = dict(p_dropout=0.1,
                         activation_class_name=config["activation_class_name"],
                         activation_class_kwargs={},
                         norm_class_name='Identity',
                         norm_class_kwargs={},
                         residual=False,
                         dropout=True,
                         activation=True,
                         inplace=True)

    # parameters for final multiplayer-perceptron
    final_mlp_kwargs = dict(width_factor=1,
                            n_layers=2,
                            loss_class_name=config["loss_class_name"],
                            loss_class_kwargs=dict())

    if target == "failing":
        final_mlp_kwargs.update(width_factor=1.5, n_layers=0, classification_model=True)

    if target == "offset":
        final_mlp_kwargs.update(width_factor=1.5, n_layers=0, classification_model=False)

    # parameters for tree-layer MLPs
    tree_layer_kwargs = dict(width_factor=1,
                             n_layers=2)

    # parameters for node-type MLPs
    node_type_kwargs = dict(width_factor=1,
                            n_layers=2,
                            one_hot_embeddings=True,
                            max_emb_dim=32,
                            drop_whole_embeddings=False)

    final_mlp_kwargs.update(**fc_out_kwargs)
    tree_layer_kwargs.update(**fc_out_kwargs)
    node_type_kwargs.update(**fc_out_kwargs)

    # Setting seeds for torch and numpy
    th.manual_seed(42)
    np.random.seed(42)

    output_dim = 1
    hidden_dim = 128

    model = ZeroShotModel(hidden_dim=hidden_dim,  # dimension of hidden vector/state
                          final_mlp_kwargs=final_mlp_kwargs,
                          node_type_kwargs=node_type_kwargs,
                          output_dim=output_dim,
                          feature_statistics=feature_statistics,
                          tree_layer_name=config["tree_layer_name"],
                          tree_layer_kwargs=tree_layer_kwargs,
                          device=device,
                          label_norm=label_norm,
                          mp_scheme=config["message_passing_scheme"],
                          readout_mode=config["readout_mode"],
                          featurization=featurization)

    # Move model to GPU
    model = model.to(model.device)

    if "WANDB_MODE" in os.environ.keys() and os.environ['WANDB_MODE'] != 'offline':
        # Watch gradients in wandb
        wandb.watch(model, log_freq=100)

    optimizer_kwargs = dict(lr=float(config["lr"]))
    optimizer_class_name = 'AdamW'
    optimizer = optim.__dict__[optimizer_class_name](model.parameters(), **optimizer_kwargs)

    # Prepare various metrics for train, validation and test
    metrics = init_metrics(target)

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = load_checkpoint(model, model_dir,
                                                                                                   model_name,
                                                                                                   optimizer=optimizer,
                                                                                                   metrics=metrics,
                                                                                                   filetype='.pt',
                                                                                                   device=device)
    if model_state == "best":  # todo?
        early_stop_m = find_early_stopping_metric(metrics)
        print("Reloading best model")
        model.load_state_dict(early_stop_m.best_model)

    checkpoint = dict(csv_stats=csv_stats, epochs_wo_improvement=epochs_wo_improvement, epoch=epoch, model=model,
                      optimizer=optimizer, metrics=metrics, finished=finished)

    return checkpoint


def init_metrics(target: str):
    metrics = dict()
    if target in [LABEL.BACKPRESSURE, LABEL.FAIL]:
        metrics["val"] = [
            Accuracy(metric_prefix="val", early_stopping_metric=True),
            F1Score(metric_prefix="val"),
            Recall(metric_prefix="val")]
        metrics["train"] = [
            Accuracy(metric_prefix="train"),
            F1Score(metric_prefix="train"),
            Recall(metric_prefix="train")]
        metrics["test"] = [
            Accuracy(metric_prefix="test"),
            F1Score(metric_prefix="test"),
            Recall(metric_prefix="test")]

    elif target in [LABEL.PLAT, LABEL.ELAT, LABEL.TPT]:
        metrics["val"] = [
            RMSE(metric_prefix="val"),
            QError(metric_prefix="val", percentile=50, early_stopping_metric=True),
            QError(metric_prefix="val", percentile=95),
            QError(metric_prefix="val", percentile=100)
        ]
        for type in ["train", "test"]:
            metrics[type] = [
                RMSE(metric_prefix=type),
                QError(metric_prefix=type, percentile=50),
                QError(metric_prefix=type, percentile=95),
                QError(metric_prefix=type, percentile=100)
            ]
    else:
        raise Exception("Metric not supported")
    return metrics


def train(config: dict, feature_statistics: dict, train_dataset: GraphDataset, val_dataset: GraphDataset,
          test_dataset: GraphDataset, model_dir: str, target: str, checkpoint, featurization):
    """ Trains a given model for a given metric  on the train_dataset while validating on val_dataset"""

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = \
        itemgetter("csv_stats", "epochs_wo_improvement", "epoch", "model", "optimizer", "metrics", "finished") \
            (checkpoint)

    print("Training with: " + str(len(train_dataset)) + " queries")
    print("Dataset sizes: "
          + "Train: " + str(len(train_dataset))
          + ", Test: " + str(len(test_dataset))
          + ", Val: " + str(len(val_dataset)))

    dataloader_args = dict(batch_size=config["batch_size"],
                           shuffle=True,
                           num_workers=config["num_workers"],
                           pin_memory=config["pin_memory"],
                           collate_fn=functools.partial(
                               collator,
                               feature_statistics=feature_statistics, target=target))

    train_loader = DataLoader(preprocess_dataset(train_dataset, feature_statistics, target, config, featurization), **dataloader_args)
    val_loader = DataLoader(preprocess_dataset(val_dataset, feature_statistics, target, config, featurization), **dataloader_args)
    test_loader = DataLoader(preprocess_dataset(test_dataset, feature_statistics, target, config,featurization), **dataloader_args)

    epochs = int(wandb.config["epochs"])
    early_stopping_epochs = int(wandb.config["early_stopping_patience"])

    while epoch < epochs:
        print(f"--- Epoch {epoch + 1}/{epochs} --- \t\n")
        epoch_stats = dict()
        epoch_stats.update(epoch=epoch)
        epoch_start_time = time.perf_counter()

        train_epoch(epoch_stats, train_loader, model, optimizer, metrics["train"])
        any_best_metric = validate_model(config, target, val_loader, model, epoch=epoch, epoch_stats=epoch_stats,
                                         metrics=metrics["val"],
                                         mode="Validation", model_dir=model_dir)

        epoch_stats.update(epoch=epoch, epoch_time=time.perf_counter() - epoch_start_time)
        wandb.log(epoch_stats)

        # see if we can already stop the training
        stop_early = False
        if not any_best_metric:
            epochs_wo_improvement += 1
            if early_stopping_epochs is not None and epochs_wo_improvement > early_stopping_epochs:
                stop_early = True
        else:
            epochs_wo_improvement = 0

        # also set finished to true if this is the last epoch
        if epoch == epochs - 1:
            stop_early = True

        epoch_stats.update(stop_early=stop_early)
        print(f"--> Epochs without improvement: {epochs_wo_improvement}/{early_stopping_epochs}")

        # save stats to file
        csv_stats.append(epoch_stats)

        # save current state of training allowing us to resume if this is stopped
        save_checkpoint(epochs_wo_improvement, epoch, model, optimizer, model_dir,
                        wandb.config["model_name"], metrics=metrics, csv_stats=csv_stats, finished=stop_early)
        epoch += 1

        if stop_early:
            print("Early stopping kicked in due to no improvement in "
                  + str(wandb.config["early_stopping_patience"]) + "epochs")
            break

    print("--- Training finished ---")
    test_stats = dict()  # copy(param_dict)
    early_stop_m = find_early_stopping_metric(metrics)

    print("Reloading best model")
    model.load_state_dict(early_stop_m.best_model)

    print("Evaluate test set with best checkpoint")
    validate_model(config=config, target=target, val_loader=test_loader, model=model, epoch=epoch,
                   epoch_stats=test_stats, metrics=metrics["test"], mode="test", model_dir=model_dir)

    return early_stop_m.best_seen_value


def validate_model(config: dict, target: str, val_loader: DataLoader, model: ZeroShotModel, epoch=0, epoch_stats=None,
                   metrics=None,
                   verbose=False, mode=None, model_dir=None):
    model.eval()

    labels = th.tensor([], device=model.device)
    outputs = th.tensor([], device=model.device)
    names = []

    with th.autograd.no_grad():
        for batch_idx, batch in enumerate(tqdm(val_loader, desc="Validation Loader")):
            graph, label = batch_to(batch, model.device, model.label_norm)

            output = model(graph)
            outputs = th.cat((outputs, output), 0)
            labels = th.cat((labels, label), 0)

            if mode in ["test", "test_other"]:
                names = names + graph[0].names

        # Compute common loss for all outputs and labels
        val_loss = float(model.loss_fxn(outputs, labels))

        # Convert labels and outputs to numpy
        labels, outputs = labels.detach().cpu().numpy(), outputs.detach().cpu().numpy()

        # Do inverse transform of labels and outputs
        if model.label_norm is not None:
            outputs = model.label_norm.inverse_transform(outputs).reshape(-1)
            labels = model.label_norm.inverse_transform(labels.reshape(-1, 1)).reshape(-1)

        if mode == "Validation":
            print(f'val_loss:\t\t\t {round(val_loss, 4)}')
            epoch_stats.update(val_loss=val_loss)

        # save best model for every metric
        any_best_metric = False
        if metrics is not None:
            for metric in metrics:
                best_seen = metric.evaluate(metrics_dict=epoch_stats, model=model, labels=labels, preds=outputs)
                if best_seen and metric.early_stopping_metric:
                    any_best_metric = True
                    best_metric = metric.metric_name

        if any_best_metric and best_metric:
            print(f"--> Found new best model for {best_metric}")

        if verbose:
            print(f'labels: {labels}')
            print(f'preds: {outputs}')

        # Log all predictions from test set to wandb & csv
        if mode in ["test", "test_other"]:
            rows = evaluate_test_set(names, labels, outputs, epoch_stats, model_dir, target, config)

            if mode == "test":
                save_csv(rows, target_csv_path=os.path.join(model_dir, f'{config["model_name"]}-pred.csv'))

                if os.environ['WANDB_MODE'] != 'offline':
                    wandb.log({"Test Predictions": wandb.Table(dataframe=pd.DataFrame(rows))})
                    wandb.log({"Test Score": wandb.Table(columns=["Metric", "Value"], data=list(epoch_stats.items()))})

            elif mode == "test_other":
                return rows

    return any_best_metric


def evaluate_test_set(names, labels, outputs, epoch_stats, model_dir, target, config):
    """ Write down test-set predictions to file and to wandb"""
    rows = []
    for (name, label, output) in zip(names, labels, outputs):
        entry = dict(name=name, real_value=label[0], prediction=output[0])
        if target in [LABEL.TPT, LABEL.ELAT, LABEL.PLAT]:
            entry.update(qerror=QError().evaluate_metric(label, output))
        else:
            entry.update(accuracy=Accuracy().evaluate_metric(label, output))
        rows.append(entry)
    return rows


def find_early_stopping_metric(metrics):
    potential_metrics = []
    for metric_type in metrics.values():
        for m in metric_type:
            if m.early_stopping_metric:
                potential_metrics.append(m)
    assert len(potential_metrics) == 1
    early_stopping_metric = potential_metrics[0]
    return early_stopping_metric


def optuna_intermediate_value(metrics):
    for m in metrics:
        if m.early_stopping_metric:
            assert isinstance(m, QError)
            return m.best_seen_value
    raise ValueError('Metric invalid')


def train_epoch(epoch_stats, train_loader, model, optimizer, metrics):
    """Train a single epoch with the train_loader. Store stats in epoch_stats"""
    model.train()
    # train_q_loss = th.Tensor([0], device=model.device)
    labels = th.tensor([], device=model.device)
    outputs = th.tensor([], device=model.device)

    for batch_idx, batch in enumerate(tqdm(train_loader, desc='Train Loader')):
        graph, label = batch_to(batch, model.device, model.label_norm)
        optimizer.zero_grad()

        output = model(graph)
        loss = model.loss_fxn(output, label)

        # Do checks of predictions and loss
        if th.isnan(output).any():
            raise ValueError("Model Predictions contain NaN:" + str(output))
        elif th.isinf(output).any():
            raise ValueError("Model Predictions contain inf:" + str(output))
        if th.isnan(loss):
            raise ValueError(f'Loss was NaN for pred: {output} and label: {label}')
        loss.backward()

        # Do gradient clipping here
        # th.nn.utils.clip_grad_value_(model.parameters(), clip_value=0.01)
        # th.nn.utils.clip_grad_norm_(model.parameters(), max_norm=2.0, norm_type=2)

        optimizer.step()
        outputs = th.cat((outputs, output), 0)
        labels = th.cat((labels, label), 0)

    # Compute loss for all predictions and labels
    train_loss = float(model.loss_fxn(outputs, labels))
    print(f'train_loss: \t\t {round(train_loss, 4)}')
    epoch_stats.update(train_loss=train_loss)

    # Convert labels and outputs to numpy arrays
    labels, outputs = labels.detach().cpu().numpy(), outputs.detach().cpu().numpy()

    # Optionally do inverse transform of labels and outputs
    if model.label_norm is not None:
        outputs = model.label_norm.inverse_transform(outputs).reshape(-1)
        labels = model.label_norm.inverse_transform(labels.reshape(-1, 1)).reshape(-1)

    # Evaluate other metrics on training data
    if metrics is not None:
        for metric in metrics:
            metric.evaluate(metrics_dict=epoch_stats, model=model, labels=labels, preds=outputs)
    return
