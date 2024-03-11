import configparser
import functools
import os
import sys
from operator import itemgetter

from learning.constants import LABEL, FULL_FEATURIZATION, NO_SEL_FEATURIZATION
from learning.training.checkpoint import save_csv

from learning.dataset.dataset import GraphDataset
from learning.dataset.dataset_creation import collator, load_graphs_from_disk, preprocess_dataset
from learning.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from learning.training.train import prepare_model, validate_model
import warnings
import argparse

from torch.utils.data import DataLoader

warnings.filterwarnings('ignore', category=UserWarning, message='TypedStorage is deprecated')


def predict(test_path: str, training_path: str, model_id: str, device="cpu", config_path="",
                config={}, model_name: str = None, query_files=[], do_save_csv=True):
    if not config:
        config = dict()
        parser = configparser.RawConfigParser()
        parser.read(config_path)
        assert os.path.exists(config_path)
        config.update(dict(parser["wandb"]))

    target = config["target"]
    print(f"Obtaining predictions for: {target}")

    if config["target"] == LABEL.FAIL:
        featurization = NO_SEL_FEATURIZATION
        exclude_failing_queries = False
    else:
        featurization = FULL_FEATURIZATION
        exclude_failing_queries = True

    model_state = "best"
    if "retrain" in model_id:
        model_state = "latest"

    if not model_name:
        hostname = os.uname()[1]
        model_name = hostname + "-" + device + "-" + config["target"] + "-" + model_id

    paths = dict(graphs=training_path,
                 stats=training_path + "/statistics.json",
                 model_dir=training_path + "/models/" + model_id + "/")

    if not os.path.exists(paths["model_dir"]):
        raise RuntimeError(f'Path {paths["model_dir"]} not found on disk.')

    feature_statistics = load_or_create_feat_statistics(paths)

    checkpoint = prepare_model(
        config=config,
        target=target,
        feature_statistics=feature_statistics,
        model_dir=paths["model_dir"],
        model_name=model_name, model_state=model_state, device=device,
        label_norm=None,
        featurization=featurization)

    # Load all graphs from disk
    graphs = load_graphs_from_disk(source_path=test_path,
                                   exclude_failing_queries=exclude_failing_queries,
                                   stratify_failing=False,
                                   stratify_offsets=False,
                                   filters=None,
                                   query_files=query_files)

    dataset = GraphDataset(graphs)
    batch_size = 32
    dataloader_args = dict(batch_size=batch_size,
                           shuffle=False,
                           collate_fn=functools.partial(
                               collator,
                               feature_statistics=feature_statistics, target=target))

    test_loader = DataLoader(preprocess_dataset(dataset, feature_statistics, target, config, featurization), **dataloader_args)

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = \
        itemgetter("csv_stats", "epochs_wo_improvement", "epoch", "model", "optimizer", "metrics", "finished") \
            (checkpoint)

    results = validate_model(config=config,
                             target=target,
                             val_loader=test_loader,
                             model=model,
                             epoch=epoch,
                             epoch_stats=dict(),
                             metrics=metrics["test"],
                             mode="test_other",
                             model_dir=paths["model_dir"])

    if do_save_csv:
        save_csv(results, target_csv_path=os.path.join(test_path, f'{model_id}-pred.csv'))
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_id', required=True)
    parser.add_argument('--test_path', default=None, required=True)
    parser.add_argument('--device', default="cpu", required=False)
    parser.add_argument('--config_path', default=None, required=True)
    parser.add_argument('--training_path', default=None, required=True)  # to find statistics.json
    args = parser.parse_args()

    predict(config_path=args.config_path, test_path=args.test_path, training_path=args.training_path,
            device=args.device, model_id=args.model_id)

    sys.exit()
