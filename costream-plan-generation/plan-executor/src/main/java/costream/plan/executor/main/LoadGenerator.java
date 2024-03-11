package costream.plan.executor.main;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import costream.plan.executor.utils.EnrichedQueryGraph;
import costream.plan.executor.utils.DiskReader;
import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.thrift.TException;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class LoadGenerator {
    private final Nimbus.Client client;
    private static final Logger logger = LoggerFactory.getLogger("load-gen");
    private final ArrayList<String> activeHostLoads;
    private final ArrayList<String> activeNetworkLoads;
    private final String sshUserName;
    private final Properties sshProps;
    private final JSch jsch;
    private final Config config;

    public LoadGenerator(Nimbus.Client client, Config config) throws JSchException {
        this.client = client;
        this.activeHostLoads = new ArrayList<>();
        this.activeNetworkLoads = new ArrayList<>();

        this.sshUserName = getSshUser(config);
        String keyPath = getSshKeyPath(config);

        this.sshProps = new Properties();
        this.sshProps.put("StrictHostKeyChecking", "no");
        this.config = config;

        this.jsch = new JSch();
        logger.info("Reading identity key: {} as file-owner {} and ssh-user {}", keyPath, System.getProperty("user.name"), this.sshUserName);
        jsch.addIdentity(keyPath);

    }

    private static String getSshUser(Config config) {
        if (config.containsKey("ssh.user") && config.get("ssh.user") != null) {
            return (String) config.get("ssh.user");
        } else {
            return System.getProperty("user.name");
        }
    }

    private static String getSshKeyPath(Config config) {
        if (config.containsKey("ssh.key.path") && config.get("ssh.key.path") != null) {
            return (String) config.get("ssh.key.path");
        } else {
            return System.getenv("HOME") + "/.ssh/id_rsa";
        }
    }

    public void generateLoad(EnrichedQueryGraph query) throws TException, IOException, JSchException, InterruptedException {
        Graph updatedQuery = DiskReader.readGraphFromDir(query.getName(), (String) this.config.get("input.path"));
        ArrayList<HostLoadProfile> hostLoads = getHostLoads(updatedQuery, config);
        ArrayList<NetworkLoadProfile> networkLoads = getNetworkLoads(updatedQuery, config);
        executeHostLoads(hostLoads);
        executeNetworkLoads(networkLoads);
    }

    private static ArrayList<HostLoadProfile> getHostLoads(Graph query, Config config) {
        ArrayList<HostLoadProfile> hostLoadProfiles = new ArrayList<>();
        for (Iterator<Node> it = query.nodes().iterator(); it.hasNext(); ) {
            Node host = it.next();
            if (host.hasAttribute(Constants.HostProperties.RAM) && host.hasAttribute(Constants.HostProperties.CPU)) {
                int ramUsage = host.getAttribute(Constants.HostProperties.RAM, Double.class).intValue();
                int cpuUsage = host.getAttribute(Constants.HostProperties.CPU, Double.class).intValue();

                if (ramUsage == 0 && cpuUsage == 0) {
                    logger.info("No hardware load to be assigned to {}/{}", host.getId(), config.get(host.getId()));
                } else {
                    hostLoadProfiles.add(new HostLoadProfile(
                            host.getId(),
                            (String) config.get(host.getId()),
                            host.getAttribute(Constants.HostProperties.CPU, Double.class).intValue(),
                            host.getAttribute(Constants.HostProperties.RAM, Double.class).intValue()));
                }
            }
        }
        return hostLoadProfiles;
    }

    private static ArrayList<NetworkLoadProfile> getNetworkLoads(Graph query, Config config) {
        ArrayList<NetworkLoadProfile> networkLoadProfiles = new ArrayList<>();
        for (Iterator<Edge> it = query.edges().iterator(); it.hasNext(); ) {
            Edge networkConnectionEdge = it.next();
            if (networkConnectionEdge.hasAttribute(Constants.HostProperties.BANDWIDTH)) {
                int networkUsage = networkConnectionEdge.getAttribute(Constants.HostProperties.BANDWIDTH, Double.class).intValue();
                //int maxBandWidth =  networkConnectionEdge.getAttribute("bandwidth", Double.class).intValue();
                if (networkUsage == 0 || networkUsage == -1) {
                    logger.info("No network to be assigned between {} and {}",
                            config.get(networkConnectionEdge.getSourceNode().getId()),
                            config.get(networkConnectionEdge.getTargetNode().getId()));
                } else {
                    networkLoadProfiles.add(new NetworkLoadProfile(
                            (String) config.get(networkConnectionEdge.getSourceNode().getId()),
                            (String) config.get(networkConnectionEdge.getTargetNode().getId()),
                            networkUsage));
                }
            }
        }
        return networkLoadProfiles;
    }

    private void executeHostLoads(ArrayList<HostLoadProfile> hostLoads) throws JSchException, IOException, InterruptedException, TException {
        activeHostLoads.clear();
        for (Iterator<SupervisorSummary> it = client.getClusterInfo().get_supervisors_iterator(); it.hasNext(); ) {
            SupervisorSummary supervisor = it.next();
            String physicalHostName = supervisor.get_host();
            HostLoadProfile profile = findLoadProfile(hostLoads, physicalHostName);

            if (profile != null) {
                assert physicalHostName.equals(profile.getPhysicalHostName());
                logger.info("Executing LoadProfile: {}", profile);
                activeHostLoads.add(profile.getPhysicalHostName());
                String command;
                if (profile.getRamLoad() != 0) {
                    command = "stress-ng -c 0 -l "
                            + profile.getCpuLoad()
                            + " -t 200m --vm 0 --vm-bytes "
                            + profile.getRamLoad()
                            + "% --vm-hang 0 > /dev/null 2>&1 &";
                } else {
                    command = "stress-ng -c 0 -l "
                            + profile.getCpuLoad()
                            + " -t 200m > /dev/null 2>&1 &";
                }
                executeSSHCommand(command, profile.getPhysicalHostName());
            }
        }
    }

    private void executeNetworkLoads(ArrayList<NetworkLoadProfile> networkLoads) throws JSchException, IOException, InterruptedException {
        activeNetworkLoads.clear();
        for (NetworkLoadProfile networkProfile : networkLoads) {
            logger.info("Executing NetworkProfile {}", networkProfile);
            String clientCommand = "iperf3 -p 5001 -f 'm' -c "
                    + networkProfile.getPhysicalTargetHost()
                    + " -b " + networkProfile.getNetworkUsageMBitPerSec() + "m -t 1000 > /dev/null 2>&1 &";
            activeNetworkLoads.add(networkProfile.getPhysicalSourceHost());
            executeSSHCommand(clientCommand, networkProfile.getPhysicalSourceHost());
        }
    }


    public void stopLoads() throws JSchException, IOException, InterruptedException {
        String command = "pkill stress-ng";
        for (String host : activeHostLoads) {
            executeSSHCommand(command, host);
        }
        command = "kill `ps ax | grep \"iperf3 -p 5001 -f m -c\" | head -n 1 | awk '{print $1}'`";
        for (String host : activeNetworkLoads) {
            executeSSHCommand(command, host);
        }
    }

    private void executeSSHCommand(String command, String host) throws JSchException, IOException, InterruptedException {
        logger.info("Executing command: {} at {}.", command, host);

        Session session = jsch.getSession(sshUserName, host, 22);
        session.setConfig(sshProps);
        session.connect();

        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        channel.setInputStream(null);
        ((ChannelExec) channel).setErrStream(System.err);
        ((ChannelExec) channel).setPty(false);

        channel.connect();
        BufferedReader br = new BufferedReader(new InputStreamReader(channel.getInputStream()));
        String s;
        while ((s = br.readLine()) != null) {
            logger.info(s);
        }
        while (!channel.isClosed()) {
            logger.info("Waiting to close channel of {}", host);
            Thread.sleep(200);
        }
        channel.disconnect();
        session.disconnect();
        logger.info("Connection to {} closed", host);
    }

    private static HostLoadProfile findLoadProfile(ArrayList<HostLoadProfile> loadProfiles, String physicalHostName) {
        logger.info("Looking for load profile of host {}", physicalHostName);
        HostLoadProfile targetProfile = null;
        for (HostLoadProfile loadProfile : loadProfiles) {
            if (loadProfile.getPhysicalHostName().equals(physicalHostName)) {
                targetProfile = loadProfile;
            }
        }
        return targetProfile;
    }
}

class NetworkLoadProfile {
    private final int networkUsageMBitPerSec;
    private final String physicalSourceHost;
    private final String physicalTargetHost;
    public NetworkLoadProfile(String physicalSourceHostName, String physicalTargetHostName, int networkUsageMBs) {
        this.physicalSourceHost = physicalSourceHostName;
        this.physicalTargetHost = physicalTargetHostName;
        this.networkUsageMBitPerSec = networkUsageMBs;
    }

    public String getPhysicalSourceHost() {
        return physicalSourceHost;
    }

    public String getPhysicalTargetHost() {
        return physicalTargetHost;
    }
    public int getNetworkUsageMBitPerSec() {
        return networkUsageMBitPerSec;
    }
    public String toString() {
        return "{sourceHost: " + physicalSourceHost
                + ", targetHost: " + physicalTargetHost
                + ", usage (MB/s): " + networkUsageMBitPerSec
                + "}";
    }
}

class HostLoadProfile {
    private final int cpuLoad;
    private final int ramLoad;
    private final String virtualHostName;
    private final String physicalHostName;

    public HostLoadProfile(String virtualHostName, String physicalHostName, int cpuLoad, int ramLoad) {
        this.virtualHostName = virtualHostName;
        this.physicalHostName = physicalHostName;
        this.cpuLoad = cpuLoad;
        this.ramLoad = ramLoad;
    }

    public String getVirtualHostName() {
        return virtualHostName;
    }

    public String getPhysicalHostName() {
        return physicalHostName;
    }

    public int getCpuLoad() {
        return cpuLoad;
    }

    public int getRamLoad() {
        return ramLoad;
    }

    public String toString() {
        return "{p_host: " + physicalHostName
                + ", v_host: " + virtualHostName
                + ", CPU: " + cpuLoad
                + ", RAM: " + ramLoad + "}";
    }
}

