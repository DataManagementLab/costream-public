import matplotlib.pyplot as plt
import datetime

paths = []
prefix = "./new_experiments/memory_over_time/"
paths.append(("cgroup mem limit", prefix + "cgroup_memory_limit.txt"))
paths.append(("cgroup mem+swap limit",  prefix + "cgroup_swap_limit.txt"))
paths.append(("cgroup mem+swap usage",  prefix + "cgroup_swap_usage.txt"))
paths.append(("cgroup mem usage",  prefix + "cgroup_memory_usage.txt"))
paths.append(("Memory - total used mem",  prefix + "memory_total_used.txt"))

fig, axs = plt.subplots()
for (name, path) in paths:
    timestamps = []
    values = []
    with open(path) as f:
        for line in f:
            timestamp, value = line.split(",")
            timestamp = datetime.datetime.fromtimestamp(int(timestamp))
            timestamps.append(timestamp)
            value = int(value)
            if "total" in path:
                value = int(value) / (1024 ** 2)
            values.append(value)

    if name == "cgroup mem+swap limit":
        axs.plot([timestamps[0], timestamps[-1]], [values[0], values[1]], label=name, color="black", linestyle="dashed")

    elif name == "cgroup mem limit":
        axs.plot([timestamps[0], timestamps[-1]], [values[0], values[1]], label=name, color="black", linestyle="-")
    else:
        axs.scatter(timestamps, values, label=name)
        axs.plot(timestamps, values, linestyle="dashed")
    axs.set_xlabel("Timestamp")
    axs.set_ylabel("Memory Usage (in MB)")
    axs.legend(fontsize=6, loc='center left')

plt.title("Storm Metrics related to Memory")
plt.tight_layout()
plt.show()