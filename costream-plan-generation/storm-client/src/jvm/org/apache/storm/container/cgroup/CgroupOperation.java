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

package org.apache.storm.container.cgroup;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * An interface to implement the basic functions to manage cgroups such as mount and mounting a hiearchy and creating cgroups.  Also
 * contains functions to access basic information of cgroups.
 */
public interface CgroupOperation {

    /**
     * Get a list of hierarchies.
     */
    List<Hierarchy> getHierarchies();

    /**
     * get a list of available subsystems.
     */
    Set<SubSystem> getSubSystems();

    /**
     * Check if a subsystem is enabled.
     */
    boolean isSubSystemEnabled(SubSystemType subsystem);

    /**
     * get the first hierarchy that has a certain subsystem isMounted.
     */
    Hierarchy getHierarchyWithBaseSystem(SubSystemType subsystem);

    /**
     * get the first hierarchy that has a certain list of subsystems isMounted.
     */
    Hierarchy getHierarchiesWithSubSystems(List<SubSystemType> subSystems);

    /**
     * check if a hiearchy is mounted.
     */
    boolean isMounted(Hierarchy hierarchy);

    /**
     * mount a hierarchy.
     */
    void mount(Hierarchy hierarchy) throws IOException;

    /**
     * umount a heirarchy.
     */
    void umount(Hierarchy hierarchy) throws IOException;

    /**
     * create a cgroup.
     */
    void createCgroup(CgroupCommon cgroup) throws SecurityException;

    /**
     * delete a cgroup.
     */
    void deleteCgroup(CgroupCommon cgroup) throws IOException;
}
