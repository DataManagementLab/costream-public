## How to enable Apache Storm to work with cgroups
As the official documentation is outdated, here is a set of guidelines how to enable Apache Storm with cgroups.
Tested with Version 2.4.0 and an Ubuntu Server 20.04. LTS. All these changes and instructions will be contained in the source
code and the playbooks. Everything here relates to cgroup version 1.

1. The Storm tutorial and source code refer to the cgroup implementation that was used in Ret Hat Enterprise Linux 6.
However, in modern OS, the cgroup structure differs in its structure. The main difference is, that in the older version
cgroups were organized in a different hierarchy. Within the new structure, the virtual cgroup file system is organized as:
`/sys/fs/cgroup/memory/MY_CGROUP/MY_SETTINGS`, `/sys/fs/cgroup/cpu/MY_CGROUP/MY_SETTING`, etc. So a new cgroup & directory needs to be
created for every controller. Thus, the related JARs `storm-client` and `storm-server` need to be fixed and updated according to the new cgroup structure. 
2. Installing `cgroup-tools` on all hosts by calling `sudo apt-get install cgroup-tools`
3. If memory swapping needs to be enabled: Update grub-file `/etc/default/grub` with swapping-configuration:
   ```shell
   GRUB_CMDLINE_LINUX_DEFAULT="cgroup_enable=memory swapaccount=1"
   GRUB_CMDLINE_LINUX="swapaccount=1"
   ```
   Then execute `sudo update-grub` and `sudo reboot`
4. Storm offers a `cgconfig`-file that can automatically be read by some tool to create the cgroups. However, as there
   have some issues with that file, the same can be also reached in an easier way by calling:
   ```shell
   sudo cgcreate -a dsps:dsps -t dsps:dsps -g memory,cpu,cpuacct:storm_resources
   ```
   This creates a cgroup `storm-resources` for the user `dsps` that is attached to `memory`, `cpu`, and `cpuacct`.
   An additional mounting is not necessary as the common super-group is already mounted
5. To enable cgroups under Storm, various settings need to be done on the workers by editing the `storm.yaml`:
   ```yaml
   storm.resource.isolation.plugin.enable: True
   storm.cgroup.hierarchy.dir: /sys/fs/cgroup
   storm.supervisor.cgroup.rootdir: storm_resources
   storm.cgroup.resources:
   - cpu
   - memory
   storm.worker.cgroup.cpu.limit: 50
   storm.worker.cgroup.memory.mb.limit: 100
   storm.cgroup.memory.enforcement.enable: True
    ```
   Do not forget to resart the supervisors after updating these files.
6. The meaning of the setting is described as follows:
   1. `storm.worker.cgroup.cpu.limit` is setting `cpu.shares` in the cgroup that will only set the shares between 
       different workers in the same cgroup
   2. `supervisor.cpu.capacity` will limit CPU utilization `cpu.cfs_quota_us` and `cpu.cfs_period_us` by e.g. using 0.2s of 1s CPU time, Default: 400
   3. `storm.worker.cgroup.memory.mb.limit` will set the `memory.limit_in_bytes`. This sets the memory limit and also the swap limit to the same specified amount.
7. Monitoring current cgroups by the following commands:
   1. Check the storm log-files, and check the `cgexec` command that is written to the logs. The Supervisor uses this
   command to actualle start the worker under the given cgroups
   2. `systemd-cgtop` shows current cgroups with RAM and CPU utilization 
   3. Browsing the cgroup path under `/sys/fs/cgroup` gives the current cgroup settings (read-write) and metrics (read-only)
   4. It also helps to activate a Storm metric listener that gives information about the current cgroups - see Storm Docs
   5. If there are crashes, check `dmesg –T` if processes are killed by OOM-Exception
   6. If there are crashes, check the garbage collector logs
