import argparse
import configparser
import os
import logging
import ansible_runner


class Manager:
    def __init__(self, conf, operations):
        self.target = conf["clusters"]["cluster_name"]
        logging.info("Cluster name is %s", self.target)
        self.cluster_path = conf["paths"]["ssh_files_path"]
        logging.info("Cluster path is %s", self.cluster_path)
        self.operations = list(set(operations)) # make sure to only run each command once
        self.extravars = self.read_extravars(conf)

    def execute(self):
        runners = []
        if "create_cluster" not in self.operations:
            self.inventory_path = self.cluster_path + "/" + self.target + "/ansible_hosts"
            logging.info("Inventory path: " + self.inventory_path)
        else:
            self.inventory_path = None
        os.environ["ANSIBLE_CONFIG"] = self.cluster_path + "/" + self.target + "/ansible.cfg"

        if self.inventory_path and not os.path.exists(self.inventory_path):
            raise FileNotFoundError(self.inventory_path + " was not found on disk")

        if "cgroup_cpu_exp" in self.operations:
            for cpu in self.extravars["cpu"]:
                print(cpu)
                ansible_runner.run(
                    private_data_dir=os.getcwd(),
                    playbook=self.get_playbook("cgroup_cpu_exp"),
                    inventory=self.inventory_path,
                    extravars=dict(cpu=cpu))

        if "cgroup_ram_exp" in self.operations:
            for ram in self.extravars["ram"]:
                print(ram)
                ansible_runner.run(
                    private_data_dir=os.getcwd(),
                    playbook=self.get_playbook("cgroup_ram_exp"),
                    inventory=self.inventory_path,
                    extravars=dict(ram=ram))

        else:
            for operation in self.operations:
                runners.append(ansible_runner.run_async(
                    private_data_dir=os.getcwd(),
                    playbook=self.get_playbook(operation),
                    inventory=self.inventory_path,
                    extravars=self.extravars)[1])

            running = True
            while running:
                if all(runner.status == "successful" for runner in runners) or any(
                        runner.status in ["failed", "canceled"] for runner in runners):
                    running = False

            for runner in runners:
                logging.info(runner.status)

    @staticmethod
    def get_playbook(task):
        paths = []
        for d in os.listdir("playbooks"):
            playbook_path = "./playbooks/" + d + "/" + task + ".yaml"
            if os.path.exists(playbook_path):
                paths.append(playbook_path)
        if len(paths) != 1:
            raise FileNotFoundError("The following paths were found for " + task + ": ", paths)
        return paths[0]

    @staticmethod
    def read_extravars(conf):
        extravars = dict()
        for key in ["paths", "clusters", "os_cluster", "costream"]:
            if key in conf:
                extravars.update(conf[key])
        if "cpu" in extravars:
            extravars["cpu"] = extravars["cpu"].split(",")
        if "ram" in extravars:
            extravars["ram"] = extravars["ram"].split(",")
        return extravars


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(description='Assistant for Distributed System and experiments')
    parser.add_argument("--config", required=True, type=str, help="Path to config file")
    parser.add_argument("--operations", required=True, type=str, nargs="*", help="Action to take")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        raise FileNotFoundError("Config file not found at %s", args.config)
    config = configparser.ConfigParser()
    config.read(args.config)

    if config["clusters"]["is_openstack"] == True:
        if os.environ.get('OS_PROJECT_ID') is None:
            raise RuntimeError("Please source openstack environment variables first!")

    os.environ["ANSIBLE_PIPELINING"] = "True"
    os.environ["ANSIBLE_DEPRECATION_WARNINGS"] = "False"
    logging.info("Program started!")
    manager = Manager(config, args.operations)
    manager.execute()
