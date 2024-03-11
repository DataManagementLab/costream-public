import argparse
import sys

from learning.dataset.dataset_creation import create_dataset

def create_extrapolation_datasets(paths):
    ds_confs = [
        # Train on lower resources to extrapolate towards higher resources
        dict(exclude_failing_queries=True, stratify_offsets=False, name="higher_cpu_restricted", stratify_failing=False,
             filters=[dict(feature="cpu", min_inclusive=50, max_inclusive=600)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="higher_ram_restricted", stratify_failing=False,
             filters=[dict(feature="ram", min_inclusive=1000, max_inclusive=16000)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="higher_bandwidth_restricted",
             stratify_failing=False,
             filters=[dict(feature="bandwidth", min_inclusive=25, max_inclusive=3200)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="higher_latency_restricted",
             stratify_failing=False,
             filters=[dict(feature="latency", min_inclusive=1, max_inclusive=40)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="higher_cpu_restricted_strat",
             stratify_failing=True,
             filters=[dict(feature="cpu", min_inclusive=50, max_inclusive=600)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="higher_ram_restricted_strat",
             stratify_failing=True,
             filters=[dict(feature="ram", min_inclusive=1000, max_inclusive=16000)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="higher_bandwidth_restricted_strat",
             stratify_failing=True,
             filters=[dict(feature="bandwidth", min_inclusive=25, max_inclusive=3200)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="higher_latency_restricted_strat",
             stratify_failing=True,
             filters=[dict(feature="latency", min_inclusive=1, max_inclusive=40)]),

        # Train on higher resources to extrapolate towards lower resources
        dict(exclude_failing_queries=True, stratify_offsets=False, name="lower_cpu_restricted", stratify_failing=False,
             filters=[dict(feature="cpu", min_inclusive=200, max_inclusive=800)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="lower_ram_restricted", stratify_failing=False,
             filters=[dict(feature="ram", min_inclusive=4000, max_inclusive=32000)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="lower_bandwidth_restricted",
             stratify_failing=False, filters=[dict(feature="bandwidth", min_inclusive=100, max_inclusive=10000)]),

        dict(exclude_failing_queries=True, stratify_offsets=False, name="lower_latency_restricted",
             stratify_failing=False, filters=[dict(feature="latency", min_inclusive=5, max_inclusive=160)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="lower_cpu_restricted_strat", stratify_failing=True,
             filters=[dict(feature="cpu", min_inclusive=200, max_inclusive=800)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="lower_ram_restricted_strat", stratify_failing=True,
             filters=[dict(feature="ram", min_inclusive=4000, max_inclusive=32000)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="lower_bandwidth_restricted_strat",
             stratify_failing=True, filters=[dict(feature="bandwidth", min_inclusive=100, max_inclusive=10000)]),

        dict(exclude_failing_queries=False, stratify_offsets=False, name="lower_latency_restricted_strat",
             stratify_failing=True, filters=[dict(feature="latency", min_inclusive=5, max_inclusive=160)])
    ]

    for conf in ds_confs:
        print(
            f'\nCreating dataset with name \033[1m{conf["name"]}\033[0m and config: {conf} at directory: {paths["dataset"]}')
        create_dataset(dataset_path=paths["dataset"], graph_path=paths["graphs"], **conf)
    return


def create_size_datasets(paths):
    """Experimental: Create Datasets with fixed size to study required number of training data"""
    amounts = reversed([36927, 18463, 9231, 4615, 2307, 1153, 576, 288, 144])
    ds_confs = []
    for amount in amounts:
        ds_confs.append(dict(exclude_failing_queries=True,
                             stratify_offsets=False,
                             name="size-" + str(amount),
                             stratify_failing=False,
                             filters=None,
                             limit_size=amount))

    for conf in ds_confs:
        print(
            f'\nCreating dataset with name \033[1m{conf["name"]}\033[0m and config: {conf} at directory: {paths["dataset"]}')
        create_dataset(dataset_path=paths["dataset"], graph_path=paths["graphs"], **conf)


def create_retrain_datasets(paths):
    conf = dict(exclude_failing_queries=True, stratify_offsets=False, name="retrain", stratify_failing=False)
    create_dataset(dataset_path=paths["dataset"], graph_path=paths["graphs"], **conf)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_data', default=None, required=False)
    parser.add_argument('--retrain_dataset', default=None, required=False)
    parser.add_argument('--mode', choices=["extrapolation", "size", "retrain"], required=True)
    parser.set_defaults(sync=True)

    args = parser.parse_args()
    paths = dict(graphs=args.training_data, stats=args.training_data + "/statistics.json",
                 dataset=args.training_data + "/datasets-new/")

    if args.mode == "extrapolation":
        create_extrapolation_datasets(paths)
    elif args.mode == "size":
        create_size_datasets(paths)
    elif args.mode == "retrain":
        paths.update(graphs=args.retrain_dataset, dataset=args.retrain_dataset + "/datasets/")
        create_retrain_datasets(paths)
    sys.exit()
