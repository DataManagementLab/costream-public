# COSTREAM - Model and Training Repository
**COSTREAM** is a learned cost model for Distributed Stream Processing Queries. 
This repository is the implementation of the Model training, learning and inference.
---
## Overview
1. [Installation and Set-Up](#installation)
2. [Train COSTREAM](#train)
3. [Inference](#inference)
4. [Hyperparameter Search](#sweep)
5. [Experimental Evaluation](#evaluation)
---

## Installation and Set-Up <a name="installation"></a>
1. Among `python3.8`, the following apt-packages are required:
   ```
   sudo apt-get install python3-pip graphviz graphviz-dev python3.8-dev python3.8-venv
   ```
2. Set up a virtual environment and install the dependencies.
    ```
   python3 -m venv venv 
   source venv/bin/activate
   pip3 install --upgrade setuptools
   pip3 install --upgrade pip
   pip3 install -r requirements.txt
   ````

   On macbook, installation of `pygraphviz` may fail because of missing dependencies on `graphviz` library. Try fixing this
   by specifying the path and config as follows
   ```
   export GRAPHVIZ_DIR="$(brew --prefix graphviz)"
   pip install pygraphviz \
    --config-settings=--global-option=build_ext \
    --config-settings=--global-option="-I$GRAPHVIZ_DIR/include" \
    --config-settings=--global-option="-L$GRAPHVIZ_DIR/lib"
    ```
4. Install DGL. DGL comes with two different versions.
   1. If you do not have a GPU, install the CPU-Version: `pip3 install dgl~=0.9.1`
   2. For a DGL-CUDA-install, make sure that the DGL version is compatible to your CUDA- and Python version. See also the
      [official installation instructions](https://www.dgl.ai/pages/start.html). This example is for CUDA 11.6:
      ```
      pip3 install  dgl -f https://data.dgl.ai/wheels/cu116/repo.html
      pip3 install  dglgo -f https://data.dgl.ai/wheels-test/repo.html
      ```
5. If you want to track your training runs, we highly recommend the use of [wandb.ai](), that comes with various
   advantages when training ML-models. In that case, run `wandb init` and paste your API-key to a (new) wandb-project. 
   Synchronization with wandb can be indicated with the `--sync` or `--no-sync`-flag later
6. Of course, you need a training dataset. You can generate an own dataset using the `dsps-plan-generation`-code.
   We also plan to provide our dataset.
---
## Training Procedure <a name="train"></a>
1. At first, create a test, validation and train-dataset out of the training data by calling `create_datasets.py`:
   ```
   python3 create_datasets.py --training_data /path/to/training/data
   ``` 
   As a consequence, various train-, test- and validation dataset are created and stored under the folder `datasets` 
   in the training data path. Having fixed datasets is required for reproducing and comparing experimental results.
   Various datasets are generated:
   - `full`: Containing all queries
   - `full_failing_strat`: Containing all queries but is stratified by failing queries to have an equal distribution
   - `non_fail`: Containing only non-failing queries
   - `non_fail_bp_strat`: Containing non-failing queries and is furthermore stratified by backpressured queries to have an equal distribution.

2. In `config` you can find various config file. A model will be trained for each of the config files in this directory.
   You can set various parameters in the config file that will be considered during training. Some are listed here:
   - `target`: Specify which target you want to train for (throughput, E2E-latency, processing-latency)
   - `loss-function`: Specify the loss function to use for training (e.g. MSLELoss, QLoss)
   - `activation-class-name`: Specify the name of the activation class  that shouldb e used (e.g. `LeakyReLU`, `CELU etc)
   
3. To actually train a Zero-Shot model, call: 
   ```
   python3 main.py --training_data /path/to/training/data --device cuda --sync --config config/
   ```
   This command uses the following flags:
   - `--device`: Either `cuda` or `cpu`.
   - `--sync` or `--no-sync`: Upload run to wandb
   - `--config`: Path to the one specific config file (`.ini`) or a complete folder with config files
4. At first the program will gather the statistics for the individual features which will later be used for the
   encoding. These are stored at `statistics.json`
5. Each model will receive a unique id under which it is logged at wandb.
6. When the number of epochs is reached or early stopping because no improvement could be achieved, the best model is loaded.
7. This best model is used for inference on the unseen test-dataset. The test results will be logged on the console and on wandb.
8. Also the model itself and its final predictions will be logged locally and on wandb under `training_data/models`

## Model Inference <a name="inference"></a>
To obtain model predictions from a given model on a given or unseen dataset, call:
```
python3 predict.py --training_path /path/to/training/data --model_id model_id --test_path ./test_data --config_path ./path/to/config_file
```
This command uses the following flags:
- `--device`: Either `cuda` or `cpu`.
- `--test_path`: Directory with queries to predict cost metrics for
- `--model_id`: ID of the model to use
- `--training_path`: Path to the training data set that the model was trained on. Under this path at `/models/{model_id}`
  the model is read. Also the statistics from this path are used.
- `--config_path`: Path to the config file that the model was trained with
---

## Hyperparameter Search <a name="sweep"></a>
In order to do a hyperparameter sweep, please have a closer look at the files under `sweep_config`.
On the one hand there are also config.ini-files which ohver have a fixed set of defined parameters as these 
are enumerated in the sweeps. The sweep configuration itself can be found in the corresponding `.yaml`-File that
holds all corresponding settings to run a sweep. 

To create a new sweep:
```
wandb sweep --project {your-project-name} sweep_config/{corresponding-yaml-file}
```
A sweep-id will be returned. To actually run the sweep, call:
```
nohup wandb agent {returned-sseep-id} --count 10 >> sweep.log &
```

## Experimental evaluation & Plotting <a name="evaluation"></a>
In order to reproduce experimental results, some material is provided:
- Under `experiments` you can find the script `create_experimental_datasets.py` that is used to create datasets for extrapolation study
 and the study about required training data.
- Furthermore, we provide configuration files to do a Hyperparameter search.
- The `optimizer` that is used for optimizing operator placements can be found under `experiments`.
- To evaluate COSTREAM and reproduce the results of the paper, please see `plotting` for the scripts that generated the
plots.
---
## Troubleshooting
- Please note that graphviz needs to be installed, which is a separate program. Sometimes, pip has problems to identify
   the location of graphviz when installing pygraphviz. In that case, make sure to export and pass the graphviz-installation 
   path properly. Please find out the installation path and call:
   ```
   export GRAPHVIZ_DIR=/opt/homebrew/Cellar/graphviz/7.1.0
   python3 -m pip install pygraphviz --global-option=build_ext --global-option="-I$GRAPHVIZ_DIR/include" --global-option="-L$GRAPHVIZ_DIR/lib
   ```
  - It can happen, that pip3 is actually not tied to the current venv and thus the packages are not installed in that.
  In this case, try installing the packages with: `python3 -m pip install -r requirements.txt`.
