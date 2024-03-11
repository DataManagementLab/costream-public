import os
import string
import sys
from datetime import datetime
import random

from learning.dataset.dataset_creation import obtain_label_normalizer, load_dataset
from learning.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from learning.training.train import prepare_model, train
from learning.constants import LABEL, NO_SEL_FEATURIZATION, FULL_FEATURIZATION
import warnings
import argparse
import wandb
import configparser

warnings.filterwarnings('ignore', category=UserWarning, message='TypedStorage is deprecated')


def train_with_config(config_path: str, args):
    config = dict()
    parser = configparser.RawConfigParser()
    parser.read(config_path)
    config.update(dict(parser["wandb"]))

    if config["target"] not in [LABEL.ELAT, LABEL.PLAT, LABEL.TPT, LABEL.BACKPRESSURE, LABEL.FAIL]:
        raise Exception("Metric not known")

    if "model_id" in config.keys():
        model_id = config["model_id"]
        print("Try to resume previous run with id: " + model_id)

    else:
        # Create random ID to later identify the run and also to store the model distinct from previous runs
        model_id = ''.join(random.choice(string.ascii_uppercase) for i in range(5))

    hostname = os.uname()[1]
    model_name = hostname + "-" + args.device + "-" + config["target"] + "-" + model_id

    paths = dict(graphs=args.training_data,
                 stats=args.training_data + "/statistics.json",
                 model_dir=args.training_data + "/models/" + model_id + "/",
                 dataset=args.training_data + "/datasets/")

    # Create feature statistics and write to file or load statistics from file
    feature_statistics = load_or_create_feat_statistics(paths)

    # Create dataset configuration and load the dataset
    dataset_args = dict(dataset_path=paths["dataset"], name=config["dataset"])
    train_dataset, val_dataset, test_dataset = load_dataset(**dataset_args)

    if args.device == "cpu":
        config.update(num_workers=0, pin_memory=False, batch_size=8)
    elif args.device in ["cuda", "cuda:0"]:
        config.update(num_workers=5, pin_memory=True, batch_size=128)
    else:
        raise ValueError("Device not supported")

    config.update(
        model_name=model_name,
        model_id=model_id,
        target=config["target"],
        hostname=hostname,
        size_training_data=len(train_dataset),
        size_validation_data=len(val_dataset),
        size_test_data=len(test_dataset),
        path=args.training_data,
        device=args.device)

    # Create a new wandb run
    print("--- Run " + model_name + " from config file " + config_path + " started at: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " ---")
    run = wandb.init(project=args.project, name=model_name, config=config, id=model_id, resume=True)

    # initialize label_normalizer
    if config["label_norm"] == "yes":
        label_norm = obtain_label_normalizer(train_dataset, val_dataset)
    else:
        label_norm = None

    if wandb.config:
        config.update(**wandb.config)

    if config["target"] == LABEL.FAIL:
        featurization = NO_SEL_FEATURIZATION
    else:
        featurization = FULL_FEATURIZATION

    # load the current model if one exists. If not, create a new one
    checkpoint = prepare_model(config=config,
                               target=config["target"],
                               feature_statistics=feature_statistics,
                               model_dir=paths["model_dir"],
                               model_name=model_name,
                               model_state="current",
                               device=wandb.config["device"],
                               label_norm=label_norm,
                               featurization=featurization)

    # Train the model on the given datasets
    best_score = train(config=config,
                       feature_statistics=feature_statistics,
                       train_dataset=train_dataset,
                       val_dataset=val_dataset,
                       test_dataset=test_dataset,
                       model_dir=paths["model_dir"],
                       target=config["target"],
                       checkpoint=checkpoint,
                       featurization=featurization)

    run.finish()
    print("--- Run " + model_name + " from config file " + config_path + " ended at: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " ---")

    # return best score for hyperparameter sweeps
    return best_score


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_data', default=None, required=True)
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--config', default=None, required=True) # either file or directory
    parser.add_argument('--sync', action='store_true')
    parser.add_argument('--no-sync', dest='sync', action='store_false')
    parser.add_argument('--model_name', required=False)  # continue previous model
    parser.add_argument('--project', required=False) # wandb-project
    parser.set_defaults(sync=True)
    args = parser.parse_args()

    if not args.sync:
        os.environ['WANDB_MODE'] = 'offline'
    else:
        os.environ['WANDB_MODE'] = 'online'

    # Execute single config file
    if os.path.isfile(args.config) and args.config.endswith(".ini"):
        train_with_config(args.config, args)

    # Execute batch of config files
    else:
        for file in sorted(os.listdir(args.config)):
            if file.endswith("ini"):
                path = os.path.join(args.config, file)
                train_with_config(path, args)
    sys.exit()
