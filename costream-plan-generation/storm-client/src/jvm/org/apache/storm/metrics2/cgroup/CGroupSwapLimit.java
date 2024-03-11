package org.apache.storm.metrics2.cgroup;

import com.codahale.metrics.Gauge;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.metrics2.WorkerMetricRegistrant;
import org.apache.storm.task.TopologyContext;


public class CGroupSwapLimit extends CGroupMetricsBase implements WorkerMetricRegistrant {

    public CGroupSwapLimit(Map<String, Object> conf) {
        super(conf, SubSystemType.memory);
    }

    @Override
    public void registerMetrics(TopologyContext topologyContext) {
        if (enabled) {
            topologyContext.registerGauge("CGroupSwapLimit", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        long value = ((MemoryCore) core).getWithSwapLimit();
                        // report in MB
                        return (value / (1024 * 1024)) ;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }
}