import paramiko
import os
import pandas as pd
import json
from tqdm import tqdm
import socket

pd.options.mode.chained_assignment = None  # default='warn'


class MetricGrabber():
    def __init__(self, base_name, logdir, target_folder):
        self.logdir = logdir
        self.hosts = [base_name+str(i) for i in range(1,10)]
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.query_names = self.get_query_names()
        self.target_folder = target_folder

    """Get list of all queries that are logged """
    def get_query_names(self):
        self.client.connect(hostname=self.hosts[0], username="ubuntu", key_filename="/home/ubuntu/.ssh/id_rsa")
        sftp_client = self.client.open_sftp()
        return sftp_client.listdir(self.logdir)

    def grab_metrics(self):
        os.makedirs(self.target_folder, exist_ok=True)
        for host in self.hosts:
            print("Grab Metrics from: " + host)
            self.client.connect(hostname=host, username="ubuntu", key_filename="/home/ubuntu/.ssh/id_rsa")
            sftp_client = self.client.open_sftp()
            for query in tqdm(self.query_names):
                source = self.logdir+query+'/6700/worker.log.metrics'
                target = self.target_folder+query.split("-")[0]+".metrics"
                try:
                    if not sftp_client.open(source).stat().st_size == 0:
                        sftp_client.get(source, target)
                except IOError:
                    tqdm.write("File " + source +" not found on " + host)
            sftp_client.close()

class MetricParser():
    def __init__(self, target_folder, basename):
        self.basename = basename
        self.target_folder = target_folder

    def parse_metrics(self):
        self.parse_to_dicts()
        #self.reduce_query_dicts(dictionaires)

    """Takes list of target files, reads them and slices the entries, stores them back to disk as list of dicts"""
    def parse_to_dicts(self):
        print("Parsing Storm logfiles to dictionaires and store them back on disk")
        result = {}
        for file in tqdm(sorted(os.listdir(self.target_folder))):
            file_reduced = []
            with open(self.target_folder+file, "r") as f:
                data = f.readlines()
            for line in data:
                line = line.split("\t")
                line_dict = {}
                line_dict["date"] = line[0].split(" ")[0]
                line_dict["time"] = line[0].split(" ")[1]
                line_dict["host"] = line[1].replace(" ","").split(":")[0]
                line_dict["thread"] = line[2].replace(" ","").split(":")[1].replace("__","")
                line_dict["metric"] = line[3].replace(" ","").replace("__","")
                line_dict["value"] = line[4]
                file_reduced.append(line_dict)
            f.close()
            with open(self.target_folder+file, 'w') as f:
                json.dump(file_reduced, f)
            f.close()

    """Takes dicts files with multiple entries, operators.filter empty values, get means"""
    def reduce_query_dicts(self):
        print("Analyzing data and reducing unnecessary entries, build means over time and write to csv")
        count = 0
        result = {}
        for file in tqdm(sorted(os.listdir(self.target_folder))):
            data = []
            with open(self.target_folder+file, 'r') as f:
                data = json.load(f)

            try:
                df = pd.DataFrame(data)
                # Delete empty entries, acker entries, system-receives
                df = df[df.value != "{}\n"]
                df = df[df.thread != "acker"]
                df = df[~((df.thread == "system") & (df.metric == "receive"))]
                # operators.map of hosts to their operators
                op_to_host_map = df[["host", "thread"]][df.thread != "system"].drop_duplicates().set_index("host")["thread"].to_dict()

                # Get list of hosts that do not host an operator (but have logging through system and acking operator)
                delete_hosts = set(list(df.host.unique())) - set(list(op_to_host_map.keys()))
                for d_host in delete_hosts:
                    df = df[df.host != d_host]

                bolts = [x for x in list(df.thread.unique()) if x.startswith("bolt")]
                spouts = [x for x in list(df.thread.unique()) if x.startswith("spout")]

                # extract throughputs
                tp = df.query('metric == "receive" or metric == "transfer"')

                # parse value column
                tp.value = tp.value.apply(lambda x:float(x.split(",")[1].split("=")[1]))
                # replace host description through operator name
                tp["operator"] = tp.host.apply(lambda x:op_to_host_map[x])
                tp.drop(["host", "thread"], axis=1, inplace=True)
                tp = tp[~((tp.operator.isin(spouts)) & (tp.metric == "receive"))]

                # compute  throughputs
                throughputs = tp[tp.operator.isin(spouts) | tp.operator.isin(bolts)].groupby([tp.operator, tp.metric]).agg({'value': ['mean', 'std']})
                #latencies = df.query('metric == "process-latency" or metric == "execute-latency"')
                #print(file.split("."))
                result[file.split(".")[0]] = throughputs

            except AttributeError:
                count+=1

        print(str(count) + " files are empty")
        output = pd.concat(result)
        csvFileName = self.target_folder + self.basename +"_metrics.csv"
        output.to_csv(csvFileName)
        return output


def main():
    base_name = socket.gethostname()[0:-1]
    logdir = '/var/bigdata/storm/logs/workers-artifacts/'
    target_folder = "/var/bigdata/storm/metrics/"

    metricGrapper = MetricGrabber(base_name, logdir, target_folder)
    metricGrapper.grab_metrics()

    metricParser = MetricParser(target_folder, base_name)
    metricParser.parse_metrics()
    df = metricParser.reduce_query_dicts()
    print(df.head())

if __name__ == '__main__':
    main()
    print("Program finished")