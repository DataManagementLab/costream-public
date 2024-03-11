import argparse
import os
from xml.dom.minidom import parse


def main(rspec_path: str, output_path: str, keypath: str):
    if not os.path.exists(rspec_path):
        raise FileNotFoundError("No rspec found at " + rspec_path)

    # parse rspec document
    rspec_document = parse(rspec_path)
    rspec_nodes = rspec_document.getElementsByTagName("node")

    # Obtain relevant node information from rspec nodes
    nodes_list = []
    for node in rspec_nodes:
        node_info = dict()
        node_info.update(node.getElementsByTagName("hardware_type")[0].attributes.items())
        node_info["hardware_type"] = node_info["name"]
        node_info.update(node.getElementsByTagName("host")[0].attributes.items())
        node_info.update(node.getElementsByTagName("services")[0].getElementsByTagName("login")[0].attributes.items())
        node_info.update(node.getElementsByTagName("interface")[0].getElementsByTagName("ip")[0].attributes.items())
        node_info["hostname"] = node_info["name"].split(".")[0]
        try:
            node_info["cluster_index"] = node_info["hostname"].replace("node", "")[0]
        except ValueError:
            node_info["cluster_index"] = 0

        print("Found node with settings: " + str(node_info))
        nodes_list.append(node_info)

    # group nodes by their cluster index
    node_groups = group_nodes_by_cluster(nodes_list)

    # create config files for every cluster
    for cluster_index in node_groups.keys():
        if len(node_groups.keys()) != 1:
            out_path = output_path + cluster_index
        else:
            out_path = output_path
        # Create path
        if not os.path.exists(out_path):
            os.makedirs(out_path, exist_ok=True)
        generate_sshconf(node_groups[cluster_index], out_path, keypath)
        generate_ansible_hosts(node_groups[cluster_index], out_path, keypath)
        generate_ansible_cfg(out_path, out_path + "/sshconfig")


def group_nodes_by_cluster(nodes_list: list):
    nodes_by_cluster = {}
    cluster_indexes = set()
    for node in nodes_list:
        cluster_indexes.update(node["cluster_index"])

    for cluster_index in cluster_indexes:
        cluster_nodes = []
        for node in nodes_list:
            if node["cluster_index"] == cluster_index:
                cluster_nodes.append(node)
        nodes_by_cluster[cluster_index] = cluster_nodes
    return nodes_by_cluster


def generate_ansible_hosts(nodes: list, output: str, keypath: str):
    path = output + "/ansible_hosts"
    open(path, 'w').close()
    with open(path, 'a') as file:
        file.write("[servers:vars]\n")
        file.write("ansible_ssh_private_key_file = " + keypath + "\n")
        file.write("\n")
        file.write("[servers]\n")
        for node in nodes:
            file.write(node["hostname"]+"\n")
        file.write("\n")
        file.write("[master]\n")
        file.write(nodes[0]["hostname"]+"\n")
        file.write("\n")
        file.write("[slaves]\n")
        for node in nodes[1:]:
            file.write(node["hostname"]+"\n")


def generate_sshconf(nodes: list, output: str, keypath: str):
    path = output + "/sshconfig"
    # clear file
    open(path, 'w').close()
    # write contents to file
    with open(path, 'a') as file:
        for node_info in nodes:
            file.write("Host " + node_info["hostname"] + "\n")
            file.write("Hostname " + node_info["name"] + "\n")
            file.write("User " + node_info["username"] + "\n")
            file.write("IdentityFile " + keypath + "\n")
            file.write("\n")


def generate_ansible_cfg(output: str, ssh_path: str):
    if not os.path.exists(output):
        os.makedirs(output, exist_ok=True)
    path = output + "/ansible.cfg"
    # clear file
    open(path, 'w').close()

    # write contents to file
    with open(path, 'a') as file:
        file.write("[ssh_connection]\n")
        file.write("ssh_args = -F " + ssh_path + " -o ControlMaster=auto -o ControlPersist=60s\n")
        file.write("ControlMaster = auto -o\n")
        file.write("pipelining = True")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Converting Cloud-Lab rspec to ssh-config file')
    parser.add_argument("--input", required=True, type=str, help="Path to rspec file")
    parser.add_argument("--output", required=True, type=str, help="Output directory where to store ssh-files")
    parser.add_argument("--identifier", required=True, type=str, help="SSH-identifier to name the cluster and the instances. "
                                                                      "Example:  'node' will give: node0, node1, ... "
                                                                      "as instance names")
    parser.add_argument("--keypath", required=True, type=str, help="Path to ssh-key")
    args = parser.parse_args()
    main(args.input, args.output, args.keypath)
    print("Successful!")
