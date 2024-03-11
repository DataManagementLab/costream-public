# COSTREAM Plan Generation
This is the training data generation implementation of: "COSTREAM", A learned cost model for stream processing queries on distributed and heterogeneous hardware.
---
## Prerequisites
- `maven` and `java`: For building programs locally
- **For Local execution**: The local execution has only been tested Ubuntu and MacOS and won`t probably run on Windows.
In case of Windows, please use the Windows Subsystem for Linux (WSL). Further, a local Zookeeper and Kafka instance 
is required. 
- **For Cluster execution**: A distributed Apache Storm cluster (v2.4.0) having one nimbus and multiple
supervisor instances is required. Please see the COSTREAM management toolkit that provides scripts and ansible-playbooks
to automatically deploy and configure a distributed cluster.
  - Required software: Storm, Zookeeper, Mongo and Kafka, NTP-Synchronization
  - You can check the cluster status and the corresponding with the use of Storms-UI. 
  Please make sure that `storm_ui`-service is up and running. You can find the cluster ui under 
  `https://master.adress:8081`
---
## Structure
- In this repository, you will find various programs that are built with `maven` and used to generate and execute 
DSPS-queries.
- The `plan-enumerator` package creates shallow query plans. These are stored as graphs under the `.dot`-format. 
- The `plan-executor` iterates over a list of query plans, executes them either in local mode or in cluster mode and 
collects all further logs and information, like the query costs, the data characteristics and so on. 
It makes use of [Storms Stream API](https://storm.apache.org/releases/2.4.0/Stream-API.html).
  - For this, `plan-executor` converts each query into a logical set of Storm operators, 
  passes them to the `CustomScheduler` in order to realize a defined placement according to the query
  and executes the query. The query is killed after a fixed time interval. 
  - The query labels and observations are written to MongoDB so `plan-executor`-fetches them. 
  The relevant information is  updated in the query-graph and this graph is again written to the disk.
- Both `storm-client` and `storm-server` packages are original packages from Storm v.2.4.0 but modified to obtain 
logs and observations that correspond to the execution of DSPS queries. They have to be located in the 
`/lib` and `/lib-worker/` directory of the Storm installation on all cluster nodes. 
To prepare the nodes and the database, a set of scripts is provided in another the COSTREAM management toolkit.
Make sure to restart the corresponding services and optionally clean the logs before the query execution. 
The repository also contains scripts for cleaning the cluster and database after an experiment.

The `storm-client` package is modified:
  - It logs the **grouping** of algebraic operators (`grouping`) to the database to describe, which operators are 
  grouped together into one physical spout.
  - It logs the **data characteristics** (`observations`) of some algebraic operators, like the selectivity. 
  This is done by attaching a `StreamMonitor` to the corresponding processors.
  - In particular, Storm creates `Processors` out of the operators that are specified by the user, i.e. 
  `FilterProcessor, AggregationProcessor,` etc.
  A `StreamMonitor` is attached to these processors that observes in & outgoing streams
  to provide and log data characteristics (`observations`). It is usually called when it receives and emits one or multiple events.
  By now, this work supports filter, windowed aggregations and windowed join operators.
  The `Stream` and `PairStream` class have been extended with the possibility to specify a description that
  contains relevant operator information. It is used in the corresponding `StreamMonitor`.
  In addition, the `StreamBuilder` was modified in order to log the grouping of algebraic operators. Otherwise, it
  is not obvious which operators are grouped together.
- Logging: If the program runs on a local machine, all of `grouping`, `observations`, `placement` and `labels` are stored locally
  under `logDir`. If the program runs in a distributed setup, (i.e cluster mode), then the logs are stored into the central
  MongoDB. To access e.g. the query labels directly, call from nimbus:
   ``mongo mongodb://`hostname -i`/dsps --eval "db.query_labels.find()"``
  

  The `storm-server` package is modified:
  - The `CGroupManager` was overwritten and improved to dynamically set the resources over Linux cgroups. It receives
    a set of resources from  `org.apache.storm.daemon.supervisor.BasicContainer` that it is allowed to acquire. These
    settings are then applied by using cgroups by writing the target values to the corresponding filepath at 
    `/sys/fs/cgroup`. Currently, we set the following cgroups of a Storm Supervisor machine: 
      - **CPU**: For setting the CPU amount that the Storm process gets.
      - **RAM**: Setting the memory that the process gets. We write the value `memory.limit_in_bytes`.
      - **Swap**: We also allow swapping by setting `memory.memsw.limit_in_bytes`. This value includes the
    `memory.limit_in_bytes.`
      - **Latency & Bandwidth**: The available network latency and bandwidth are set with `netem` at `BasicContainer`.

---
## Plan Enumeration
Before queries can be executed, they have to be generated using tha plan-enumerator.
To create random plans, please call:
```
sudo -u dsps java -jar plan-enumerator-1.0.0.jar --mode random --num 100 --jvm 12500 --output plans --dur 240 --hosts 5```
```
To create fixed  plans (e.g. linear with different CPU resources), please call:
```
sudo -u dsps java -jar plan-enumerator-1.0.0.jar --mode linear --num 1 --jvm 12500 --output plans --dur 240 --max 50000 --cpu 85 --cpu 90 --cpu 95 --cpu 100
```
Explanation of the flags:
- `--num`: Number of queries to generate per query type
- `--output`: Directory where to log shallow query plans
- `--hosts`: Maximal amount of (virtual) hosts to distribute operators on
- `--dur`: Duration that the query should run in seconds
- `--jvm`: Specify the JVM worker size that is assigned to the worker processes
- `--mode`: Either `random` or ot of `linear`, `linear-agg-count`, `linear-agg-duration`, `two-way-join`, `three-way-join`.
    - In `random` mode, queries are created with random data streams, operators and hardware properties.
    - In `linear` mode, linear queries are created (source --> filter --> sink)
    - In `linear-agg-count` mode, linear queries with a count-based aggregation are created
    - In `linear-agg-duration` mode, linear queries with a duration-based aggregation are created
    - `two-way-join` and `three-way-join`-modes create respectively 2-way- and 3-way-join queries.

Some other modes create benchmark queries. For exact query descriptions, see also [DSPBench](https://ieeexplore.ieee.org/document/9290133).
These following modes include randomness: Event Rates, Operator Placements and Harwdare Properties are generated
randomly.
- The `advertisement`-mode creates for each `num` three queries: A click-stream query, an impression query and a joined query.
- The `spikedetection`-mode creates queries for each `num` corresponding to the Spike Detection benchmark from DSPBench.
- The `smart-grid`-mode creates queries for each `num`corresponding to the Smart Grid benchmark.
- The `two-filter-chain`, `three-filter-chain`, `four-filter-chain` modes create a linear filter chain for experiments 


Additional dimensions for non-`random`-modes:
- `--max`: Highest event rate. This is combined with `--num` to create equi-distant event rates up to the max-value.
- `--tupleWidth`: Tuple Width for the sources
- `--ram`: Absolute RAM to be assigned in MB
- `--cpu` Absolute CPU in relative unit (100 = 1CPU Core)
- `--network`: Outgoing restricted network usage in MB/s per worker
- `--latency`: Outgoing additional latency in ms per worker
- `--window`: Window size in seconds

If you want to visualize the created or executed query-plans, please call the `graph-visualizer.py`.

---
## Plan Execution
1. If you want to **execute queries locally**, please run: 
    ```
    java jar plan-executor-1.0.0.jar --input ./plans --output ./plans-executed --logdir ./logs --localExecution True
    ```
    Make sure that Kafka and Zookeeper are running locally (default settings)
2. If you want to **execute queries on a distributed cluster**, please run:
    ```
   nohup sudo -u dsps ./bin/storm jar plan-executor-1.0.0.jar costream.plan.executor.main.PlanExecutor --input /var/bigdata/storm/plans --output /var/bigdata/storm/plans-executed --logdir ./logs --localExecution False &
    ```
   Make sure that you specify absolute paths for the `input` and `output`-directory. Various log-files are
   generated and indicate the query execution success. The query is terminated after `dur` seconds.
