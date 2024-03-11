# COSTREAM Management Toolkit
This repository contains several Ansible Playbooks to set-up and control multiple clusters of nodes to run distributed 
frameworks such as **Apache Storm**, **Apache Zookeeper**, **Apache Kafka**, **Hadoop** and **MongoDB**.

Currently, **OpenStack** and **[Cloud-Lab](https://www.cloudlab.us/)** hypervisors are supported.
This toolkit is able to create instances, interconnect them and to set-up different big-data frameworks.
Furthermore, it is used to generate training data for COSTREAM - a learned cost estimation model for Distributed Stream
Processing Systems.

## Overview
The folder `playbooks` contains several Ansible-Playbooks. In general, there are three folders of playbooks:
- `openstack`: Openstack-specific playbooks to create and destroy instances
- `infrastructure`: Setting up distributed services on a given cluster
- `costream`: Scripts to use for COSTREAM training data generation
The folder `conf` contains configuration files with metadata that is required to run the scrupts
- The folder `cluster` will be created during operation. It contains the required SSH-config-files and ansible-files.
These are required to connect to the cluster propertly.

### General Use
To run a particular playbook, e.g. the `deploy_storm` playbook against set of nodes,
run the `manager`-script with the following command:

```
python3 manager.py --config ./config.ini --operation deploy_storm
```

It has the following arguments:
- `--config`: Path to configuration file with some metadata to use in the further executions. 
See template under `conf` for more information.
- `--operation`: Name of the playbook to run, e.g. `deploy_storm`
---
## Create instances with OpenStack
In case you have access to an OpenStack-cluster and no resources are given, it is possible to create a set of instances
by using this toolkit.

- Creating a cluster:
  ```
  python3 manager.py --config ./config.ini --operation create_cluster
  ```
- The new cluster is set-up in a private network with a public IP-address, provided as floating-IP. 
This serves as entrypoint for the ssh-connections. After creation, relevant config-files are created under 
the `cluster`-folder that are later used to connect to the cluster again. Make sure that you have enough quota 
to create your nodes and that your environment file is sourced.
- Removing a cluster:
  ```
  python3 manager.py --config ./config.ini --operation remove_cluster
  ```
---
## Use instances from CloudLab
In case that you use and access instances from CloudLab, it is possible to extract a `rspec`-file from the 
ongoing experiment. Please open up your experiment and find the `.rspec`-file under the tab: `Manifest`
This file can be converted by the script `convert_rspec` to create the necessary set of config-files to connect to 
the cluster. The usage is as following. Please replace `{cluster_name}` with a meaningful name of your choice.
```
python3 convert_rspec.py --input /path/to/rspec/cluster.rspec --output ./cluster/{cluster_name} --identifier {cluster_name}
```
Please note that the CloudLab-instances are publically available and this might be attacked. 
Please consider additional security measures.

---
## Prepare a cluster
Given a cluster as a set of nodes from any hypervisor and the necessary config-files, 
this toolkits prepares the nodes to work as a cluster by the playbook:
```
python3 manager.py --config ./config.ini --operation prepare_instances
```
This playbook sets up `NTP` for all the nodes and makes them visible & SSH-accessible by each other.

---
## Install distributed frameworks
As usually given in distributed settings, the services are generally set-up in a master-slave-architecture,
where one instance works as `master` and the other as `workers`.

The following distributed frameworks are supported:
- **Apache Zookeeper**: The `master` runs a Zookeeper service that is used for other frameworks as a basis.
It runs under port 2181.
- **Apache Storm**: The `master` runs the `nimbus`-service of Storm, and the `workers` 
are running the `supervisor`-services. In addition, there is a GUI provided that runs on the `master` to display 
the cluster status. It uses the ports 6627 (Storm) and 8081 (GUI).  Storm requires a Zookeeper installation.
- **MongoDB**: A MongoDB-Server is set up as servoce `mongod` on the master. The `workers` receive a 
client installation, to query the master via the command line. Note that a password authentication is afforded.
- **Apache Kafka**: The `master` runs a Kafka Broker `kafka` that can be queried by any node. It runs under port 9092.

To install one or many frameworks, e.g. Apache Kafka and Apache Zookeeper, please call:
```
python3 manager.py --config ./config.ini --operation deploy_zookeeper deploy_kafka
```
---
## Prepare COSTREAM training data generation
- To prepare the cluster for the execution of DSPS queries and the observation of query costs, 
please call the `prepare_experiments` script.  This script uploads the modified storm jars to the cluster and makes 
necessary configurations. It furthermore enables cgroups in case they aren't yet.
- To only upload the storm-jars, call `upload_jars`
- To reset the full cluster (e.g. when a query execution has crasehd) 
it is recommended to reset the cluster with `reset_cluster`. This clears the logs,  restarts the services and clears 
the central MongoDB database.
---
## Troubleshooting
- Sometimes the playbook `prepare_isntances` hangs at the very initial `apt-update`-command.
Stopping and restarting the playbook might help here.