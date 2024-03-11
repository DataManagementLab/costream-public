import argparse
import sys

from learning.dataset.dataset_creation import create_dataset


def create_datasets(args):
    paths = dict(graphs=args.training_data, stats=args.training_data + "/statistics.json",  dataset=args.training_data + "/datasets/")
    ds_confs = [
        # Full dataset containing all queries
        dict(exclude_failing_queries=False, stratify_offsets=False, name="full", stratify_failing=False),
        # Full dataset reduced to stratify failing and non-failing queries
        dict(exclude_failing_queries=False, stratify_offsets=False, name="full_failing_strat", stratify_failing=True),
        # Dataset containing only successful queries (non-failing)
        dict(exclude_failing_queries=True, stratify_offsets=False, name="non_fail", stratify_failing=False),
        # Dataset containing only successful queries, further stratify back-pressured queries
        dict(exclude_failing_queries=True, stratify_offsets=True, name="non_fail_bp_strat", stratify_failing=False)]

    for conf in ds_confs:
        print(f'\nCreating dataset with name \033[1m{conf["name"]}\033[0m and config: {conf} at directory: {paths["dataset"]}')
        create_dataset(dataset_path=paths["dataset"], graph_path=paths["graphs"], **conf)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_data', default=None, required=True)
    parser.add_argument('--device', default='cpu')
    parser.set_defaults(sync=True)
    args = parser.parse_args()
    create_datasets(args)
    sys.exit()
