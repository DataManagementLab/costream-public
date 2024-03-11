/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.container.cgroup;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.SystemUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.container.DefaultResourceIsolationManager;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.CpusetCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.core.NetClsCore;
import org.apache.storm.daemon.supervisor.ClientSupervisorUtils;
import org.apache.storm.daemon.supervisor.ExitCodeCallback;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements ResourceIsolationInterface that manages cgroups.
 */
public class CgroupManager extends DefaultResourceIsolationManager {

    private static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);
    private CgroupCenter center;
    private CgroupCommon memoryCGroup;
    private CgroupCommon cpuCgroup;
    private CgroupCommon netClsCgroup;
    private String rootDir;
    private Map<String, String> workerToNumaId;

    /**
     * initialize data structures.
     *
     * @param conf storm confs
     */
    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        super.prepare(conf);
        this.rootDir = DaemonConfig.getCgroupRootDir(this.conf);
        if (this.rootDir == null) {
            throw new RuntimeException("Check configuration file. The storm.supervisor.cgroup.rootdir is missing.");
        }

        LOG.info("Cgroup root directory for storm is: {}", rootDir);

        for (String resource : DaemonConfig.getCgroupStormResources(this.conf)) {
            String path = DaemonConfig.getCgroupStormHierarchyDir(this.conf) + "/" + resource;
            File file = new File(path, rootDir);
            LOG.info("Resource {} found in config with path: {}", resource, path);
            if (!file.exists()) {
                LOG.error("{} does not exist", file.getPath());
                throw new RuntimeException(
                        "Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
            }
        }

        this.center = CgroupCenter.getInstance();
        if (this.center == null) {
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        }
        this.prepareSubSystem(this.conf);
        workerToNumaId = new ConcurrentHashMap();
    }

    /**
     * Initialize subsystems.
     */
    private void prepareSubSystem(Map<String, Object> conf) throws IOException {
        // --------------------- Read subsystems from config file --------------------------------
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : DaemonConfig.getCgroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        if (subSystemTypes.isEmpty()) {
            LOG.error("Did not found subsystems in conf");
            throw new RuntimeException("No subsystems found in conf");
        }
        LOG.info("Subsystems found in conf: {}", conf);

        // --------------------- Create/Load common cgroups based on the subsystems -------------------------------
        // Reading common CPU cgroup
        Hierarchy cpuHierarchy = center.getHierarchyWithBaseSystem(SubSystemType.cpu);
        if (cpuHierarchy != null) {
            this.cpuCgroup = new CgroupCommon(this.rootDir, cpuHierarchy, cpuHierarchy.getRootCgroups());
            LOG.info("Found common CPU Group: {}", this.cpuCgroup.getDir());
        }

        // Reading common memory cgroup
        Hierarchy memoryHierarchy = center.getHierarchyWithBaseSystem(SubSystemType.memory);
        if (memoryHierarchy != null) {
            this.memoryCGroup = new CgroupCommon(this.rootDir, memoryHierarchy, memoryHierarchy.getRootCgroups());
            LOG.info("Found common memory group: {}", this.memoryCGroup.getDir());
        }

        // Reading common net_cls cgroup
        Hierarchy netClsHierarchy = center.getHierarchyWithBaseSystem(SubSystemType.net_cls);
        if (netClsHierarchy != null) {
            this.netClsCgroup = new CgroupCommon(this.rootDir, netClsHierarchy, netClsHierarchy.getRootCgroups());
            LOG.info("Found common net_cls group: {}", this.netClsCgroup.getDir());
        }

        // ToDo: Not sure, if this is required with the new cgroups architecture
        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS
        // operations.
        // CpuCore supervisorRootCpu = (CpuCore) this.cpuCgroup.getCores().get(SubSystemType.cpu);
        // setCpuUsageUpperLimit(supervisorRootCpu, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }

    /**
     * Use cfs_period & cfs_quota to control the upper limit use of cpu core e.g.
     * If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {
        LOG.info("Setting CpuUsage upper limit to {}", cpuCoreUpperLimit);
        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    @Override
    public void reserveResourcesForWorker(String workerId,
                                          Integer totalMemory,
                                          Integer totalMemAndSwap,
                                          Integer cpuLimit,
                                          Integer cpuShares,
                                          Integer majorClassId,
                                          Integer minorClassId,
                                          String numaId) throws SecurityException {

        LOG.info("Receiving resources from assignment: Total memory: {}MB, total memsw: "
                        + "{}MB, CPU Limit: {}, CPU shares: {}% CPU for worker {}",
                totalMemory, totalMemAndSwap, cpuLimit, cpuShares, workerId);

        if (totalMemAndSwap < totalMemory) {
            throw new RuntimeException("swap size needs to be larger or equals than total memory size");
        }

        // --------------------- Read optional values from config  --------------------------------
        // The manually set STORM_WORKER_CGROUP_MEMORY_MB_LIMIT config on supervisor will overwrite
        // resources assigned from method call

        // Reading memory limit
        if (this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT) != null) {
            totalMemory =
                    ((Number) this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT)).intValue();
        }
        LOG.info("Applying cgroup memory limit: {} for worker {}", totalMemory, workerId);

        // Reading mem+swap limit
        if (this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_SWAP_MB_LIMIT) != null) {
            totalMemAndSwap = ((Number) this.conf.get(DaemonConfig.STORM_WORKER_CGROUP_SWAP_MB_LIMIT)).intValue();
        }
        LOG.info("Applying cgroup memsw limit: {} for worker {}", totalMemAndSwap, workerId);

        // Reading cpu limit
        //if (conf.get(Config.SUPERVISOR_CPU_CAPACITY) != null) {
        //    cpuLimit = ((Number) conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue();
        //}
        LOG.info("Applying cgroup CPU limit: {} for worker {}", cpuLimit, workerId);

        // Reading cpu shares
        if (conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            cpuShares = ((Number) conf.get(DaemonConfig.STORM_WORKER_CGROUP_CPU_LIMIT)).intValue();
        }
        LOG.info("Applying cgroup CPU shares: {} for worker {}", cpuShares, workerId);

        // --------------------- Create worker-related cgroups --------------------------------
        // Use child cgroups from here on as these contain the worker id and specify the resources for the given worker
        CgroupCommon workerCpuGroup = new CgroupCommon(workerId, this.cpuCgroup.getHierarchy(), this.cpuCgroup);
        LOG.info("Using CPU cgroup with name {}, hierarchy {} and dir {}",
                workerCpuGroup.getName(), workerCpuGroup.getHierarchy(), workerCpuGroup.getDir());

        CgroupCommon workerMemoryGroup = new CgroupCommon(workerId, this.memoryCGroup.getHierarchy(), this.memoryCGroup);
        LOG.info("Using memory cgroup with name {}, hierarchy {} and dir {}",
                workerMemoryGroup.getName(), workerMemoryGroup.getHierarchy(), workerMemoryGroup.getDir());

        CgroupCommon workerNetworkGroup = new CgroupCommon(workerId, this.netClsCgroup.getHierarchy(), this.netClsCgroup);
        LOG.info("Using net_cls cgroup with name {}, hierarchy {} and dir {}",
                workerNetworkGroup.getName(), workerNetworkGroup.getHierarchy(), workerNetworkGroup.getDir());

        try {
            this.center.createCgroup(workerCpuGroup);
            this.center.createCgroup(workerMemoryGroup);
            this.center.createCgroup(workerNetworkGroup);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating Cgroup! Exception: ", e);
        }

        // --------------------- Set values to worker cgroups  --------------------------------
        // set CPU shares
        if (cpuShares != null && cpuShares != -1) {
            CpuCore cpuCore = (CpuCore) workerCpuGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuShares.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: ", e);
            }
        } else {
            LOG.info("No CPU Shares value has been set");
        }

        // set CPU limit
        if (cpuLimit != null && cpuLimit != -1) {
            try {
                CpuCore supervisorRootCpu = (CpuCore) workerCpuGroup.getCores().get(SubSystemType.cpu);
                LOG.info("Applying cgroup CPU upper limit: {} for worker {}", cpuLimit, workerId);
                setCpuUsageUpperLimit(supervisorRootCpu, cpuLimit);
            } catch (IOException e) {
                throw new RuntimeException("Cannot set Cpu-Usage! Exception: ", e);
            }
        } else {
            LOG.info("No CPU Limit has been set");
        }

        // set memory
        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE) && totalMemory != -1) {
            if (totalMemory != null) {
                int cgroupMem =
                        (int)
                                (Math.ceil(
                                        ObjectReader.getDouble(
                                                this.conf.get(DaemonConfig.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB),
                                                0.0)));
                long memLimit = Long.valueOf((totalMemory.longValue() + cgroupMem) * 1024 * 1024);
                MemoryCore memCore = (MemoryCore) workerMemoryGroup.getCores().get(SubSystemType.memory);
                try {
                    memCore.setPhysicalUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: ", e);
                }
                long swapLimit = Long.valueOf(totalMemAndSwap) * 1024 * 1024;
                try {
                    memCore.setWithSwapUsageLimit(swapLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.memsw.limit_in_bytes! Exception: ", e);
                }
            }
        } else {
            LOG.info("No memory values have been set");
        }

        // Set network, only the class ID is set
        NetClsCore netClsCore = (NetClsCore) workerNetworkGroup.getCores().get(SubSystemType.net_cls);
        if (majorClassId != null && majorClassId != -1) {
            try {
                LOG.info("Setting net_cls id: {}:{}", majorClassId, minorClassId);
                netClsCore.setClassId(majorClassId, minorClassId);
            } catch (IOException e) {
                throw new RuntimeException("Cannot set net_cls class id! Exception: ", e);
            }
        } else {
            LOG.info("No net_cls values have been set");
        }

        // set inheritance - outdated?
        if ((boolean) this.conf.get(DaemonConfig.STORM_CGROUP_INHERIT_CPUSET_CONFIGS)) {
            if (workerCpuGroup.getParent().getCores().containsKey(SubSystemType.cpuset)) {
                CpusetCore parentCpusetCore = (CpusetCore) workerCpuGroup.getParent().getCores().get(SubSystemType.cpuset);
                CpusetCore cpusetCore = (CpusetCore) workerCpuGroup.getCores().get(SubSystemType.cpuset);
                try {
                    cpusetCore.setCpus(parentCpusetCore.getCpus());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.cpus! Exception: ", e);
                }
                try {
                    cpusetCore.setMems(parentCpusetCore.getMems());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.mems! Exception: ", e);
                }
            }
        }

        if (numaId != null) {
            workerToNumaId.put(workerId, numaId);
        }
    }

    @Override
    public void cleanup(String user, String workerId, int port) throws IOException {
        CgroupCommon memoryGroup = new CgroupCommon(workerId, this.memoryCGroup.getHierarchy(), this.memoryCGroup);
        CgroupCommon networkGroup = new CgroupCommon(workerId, this.netClsCgroup.getHierarchy(), this.netClsCgroup);
        CgroupCommon cpuGroup = new CgroupCommon(workerId, this.cpuCgroup.getHierarchy(), this.cpuCgroup);

        for (CgroupCommon group : Arrays.asList(memoryGroup, cpuGroup, networkGroup)) {
            try {
                Set<Integer> tasks = group.getTasks();
                if (!tasks.isEmpty()) {
                    throw new Exception("Cannot correctly shutdown worker CGroup " + workerId + "tasks " + tasks
                            + " still running!");
                }
                this.center.deleteCgroup(group);
            } catch (Exception e) {
                LOG.error("Exception thrown when shutting worker {} Exception: {}", workerId, e);
            }
        }
    }

    /**
     * Extracting out to mock it for tests.
     *
     * @return true if on Linux.
     */
    protected static boolean isOnLinux() {
        return SystemUtils.IS_OS_LINUX;
    }

    private void prefixNumaPinning(List<String> command, String numaId) {
        if (isOnLinux()) {
            command.add(0, "numactl");
            command.add(1, "--cpunodebind=" + numaId);
            command.add(2, "--membind=" + numaId);
            return;
        } else {
            // TODO : Add support for pinning on Windows host
            throw new RuntimeException("numactl pinning currently not supported on non-Linux hosts");
        }
    }

    @Override
    public void launchWorkerProcess(String user, String topologyId, Map<String, Object> topoConf,
                                    int port, String workerId,
                                    List<String> command, Map<String, String> env, String logPrefix,
                                    ExitCodeCallback processExitCallback, File targetDir) throws IOException {
        if (workerToNumaId.containsKey(workerId)) {
            prefixNumaPinning(command, workerToNumaId.get(workerId));
        }

        if (runAsUser) {
            String workerDir = targetDir.getAbsolutePath();
            List<String> args = Arrays.asList("worker", workerDir, ServerUtils.writeScript(workerDir, command, env));
            List<String> commandPrefix = getLaunchCommandPrefix(workerId);
            ClientSupervisorUtils.processLauncher(conf, user, commandPrefix, args, null,
                    logPrefix, processExitCallback, targetDir);
        } else {
            command = getLaunchCommand(workerId, command);
            ClientSupervisorUtils.launchProcess(command, env, logPrefix, processExitCallback, targetDir);
        }
    }

    /**
     * To compose launch command based on workerId and existing command.
     *
     * @param workerId        the worker id
     * @param existingCommand the current command to run that may need to be modified
     * @return new commandline with necessary additions to launch worker
     */
    @VisibleForTesting
    public List<String> getLaunchCommand(String workerId, List<String> existingCommand) {
        List<String> newCommand = getLaunchCommandPrefix(workerId);

        if (workerToNumaId.containsKey(workerId)) {
            prefixNumaPinning(newCommand, workerToNumaId.get(workerId));
        }

        newCommand.addAll(existingCommand);
        return newCommand;
    }

    private List<String> getLaunchCommandPrefix(String workerId) {
        // This actually returns the command prefix that uses cgexec to attach the worker to a cgroup.
        // As we might have now multiple controllers/hierarchies to that the process needs to be attached, the output
        // will look like the following example:
        // [/bin/cgexec, -g, cpu,cpuacct:/storm_resources/2f750deb-264f-4c8b-a0f4-1768c0ae73a0,
        // -g, memory:/storm_resources/2f750deb-264f-4c8b-a0f4-1768c0ae73a0]

        StringBuilder sb = new StringBuilder();
        sb.append(this.conf.get(DaemonConfig.STORM_CGROUP_CGEXEC_CMD)).append(" -g ");
        for (CgroupCommon cgroup : Arrays.asList(this.cpuCgroup, this.memoryCGroup, this.netClsCgroup)) {
            Hierarchy h = cgroup.getHierarchy();
            for (SubSystemType subSystemType : h.getSubSystems()) {
                sb.append(subSystemType.toString());
                sb.append(",");
            }
            // remove last comma
            sb.setLength(sb.length() - 1);
            sb.append(":");
            sb.append("/storm_resources");
            sb.append("/");
            sb.append(workerId);
            sb.append(" -g ");
        }
        // remove last spaces and gs
        sb.setLength(sb.length() - 4);
        List<String> newCommand = new ArrayList<String>();
        newCommand.addAll(Arrays.asList(sb.toString().split(" ")));
        LOG.info("Returning launch command prefix: {}", newCommand);
        return newCommand;
    }

    private Set<Long> getRunningPids(String workerId) throws IOException {
        Set<Long> pids = this.cpuCgroup.getPids();
        pids.addAll(this.memoryCGroup.getPids());
        return pids;
    }

    /**
     * Get all of the pids that are a part of this container.
     *
     * @param workerId the worker id
     * @return all of the pids that are a part of this container
     */
    @Override
    protected Set<Long> getAllPids(String workerId) throws IOException {
        Set<Long> ret = super.getAllPids(workerId);
        Set<Long> morePids = getRunningPids(workerId);
        assert (morePids != null);
        ret.addAll(morePids);
        return ret;
    }

    @Override
    public long getMemoryUsage(String user, String workerId, int port) throws IOException {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.memoryCGroup.getHierarchy(), this.memoryCGroup);
        //CgroupCommon workerGroup = this.memoryCGroup;
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
        return memCore.getPhysicalUsage();
    }

    @Override
    public long getSystemFreeMemoryMb() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;
        try {
            MemoryCore memRoot = (MemoryCore) this.memoryCGroup.getCores().get(SubSystemType.memory);
            if (memRoot != null) {
                //For cgroups no limit is max long.
                long limit = memRoot.getPhysicalUsageLimit();
                long used = memRoot.getMaxPhysicalUsage();
                rootCgroupLimitFree = (limit - used) / 1024 / 1024;
            }
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        return Long.min(rootCgroupLimitFree, ServerUtils.getMemInfoFreeMb());
    }

    @Override
    public boolean isResourceManaged() {
        return true;
    }
}
