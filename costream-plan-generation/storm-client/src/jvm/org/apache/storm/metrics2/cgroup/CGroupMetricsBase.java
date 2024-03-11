/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metrics2.cgroup;

import java.io.File;
import java.util.Map;

import org.apache.storm.container.cgroup.CgroupCenter;
import org.apache.storm.container.cgroup.CgroupCoreFactory;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for checking if CGroups are enabled, etc.
 */
public abstract class CGroupMetricsBase {
    private static final Logger LOG = LoggerFactory.getLogger(CGroupMetricsBase.class);
    protected boolean enabled;
    protected CgroupCore core = null;

    public CGroupMetricsBase(Map<String, Object> conf, SubSystemType type) {
        // this also needed to be updated on a recent OS version
        // See storm-server cgroup-implementation for further hints.

        final String simpleName = getClass().getSimpleName();
        enabled = false;
        CgroupCenter center = CgroupCenter.getInstance();

        if (center == null) {
            LOG.warn("{} is disabled. cgroups do not appear to be enabled on this system", simpleName);
            return;
        }
        if (!center.isSubSystemEnabled(type)) {
            LOG.warn("{} is disabled. {} is not an enabled subsystem", simpleName, type);
            return;
        }

        //Check to see if the CGroup is mounted at all
        if (null == center.getHierarchyWithBaseSystem(type)) {
            LOG.warn("{} is disabled. {} is not a mounted subsystem", simpleName, type);
            return;
        }

        //Good so far, check if we are in a CGroup
        File cgroupFile = new File("/proc/self/cgroup");
        if (!cgroupFile.exists()) {
            LOG.warn("{} is disabled we do not appear to be a part of a CGroup", getClass().getSimpleName());
            return;
        }

        String cgroupPath;
        File cgroupNewFile = getLastModified("/sys/fs/cgroup/" + type.name() + "/storm_resources");
        if (cgroupNewFile == null) {
            LOG.warn("Cgroup path is empty");
            return;
        } else {
            cgroupPath = cgroupNewFile.getAbsolutePath();
        }

        LOG.info("Working with cgroup path: {}", cgroupPath);

        /*
        File[] files = dir.listFiles(File::isDirectory);
        assert files != null;
        if (files.length > 1) {
            LOG.error("Found more than one group for storm within: " + dir);
            throw new RuntimeException("Found more than one group for storm within: " + dir);
        } else if (files.length == 0) {
            LOG.info("No worker-related cgroups found");
            return;
        }
        String cgroupPath = files[0].getPath();
         */
        //Storm on Rhel6 and Rhel7 use different cgroup settings.
        //On Rhel6, the cgroup of the worker is under
        // "Config.STORM_CGROUP_HIERARCHY_DIR/DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR/<worker-id>"
        //On Rhel7, the cgroup of the worker is under
        // "Config.STORM_OCI_CGROUP_ROOT/<subsystem>/DaemonConfig.STORM_OCI_CGROUP_PARENT/<container-id>"
        // This block of code is a workaround for the CGroupMetrics to work on both system
        //String hierarchyDir = (String) conf.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        String hierarchyDir = "/sys/fs/" + type.name() + "/storm_resources";

        LOG.info("Hierarchy dir is: {} and cgroupPath is: {}", hierarchyDir, cgroupPath);
        core = CgroupCoreFactory.getInstance(type, new File(cgroupPath).getAbsolutePath());
        enabled = true;
        LOG.info("Metric {} is ENABLED and directory {} exists...", simpleName, hierarchyDir);
    }

    public static File getLastModified(String directoryPath) {
        File directory = new File(directoryPath);
        LOG.info("Looking for worker-related cgroups in directory: {}", directory.getAbsolutePath());
        if (!directory.exists()) {
            LOG.info("No storm-related cgroups found");
            return null;
        }

        File[] files = directory.listFiles(File::isDirectory);
        if (files == null || files.length == 0) {
            return null;

        } else {
            long lastModifiedTime = Long.MIN_VALUE;
            File chosenDirectory = null;

            for (File file : files) {
                if (file.lastModified() > lastModifiedTime) {
                    chosenDirectory = file;
                    lastModifiedTime = file.lastModified();
                }
            }
            return chosenDirectory;
        }
    }
}
