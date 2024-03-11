package costream.plan.executor.kafka;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperator;
import costream.plan.executor.utils.Triple;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.*;

import java.util.HashMap;

public class KafkaSpoutOperator extends AbstractOperator<Object> {
    final String bootstrapServer;
    final int kafkaPort;
    private Triple<Integer, Integer, Integer> numTupleDatatypes; // int, double, string
    private Double eventRate;
    private final String topic;
    private final Config config;
    private final boolean localExecution;

    public KafkaSpoutOperator(String topic, String bootstrapServer, int kafkaPort, Double confEventRate, Config config) {
        super();
        this.topic = topic;
        this.bootstrapServer = bootstrapServer;
        this.kafkaPort = kafkaPort;
        this.numTupleDatatypes = new Triple<>(null, null, null);
        this.eventRate = confEventRate;
        this.config = config;
        this.localExecution = config.get("storm.cluster.mode").equals("local");
    }

    @Override
    public KafkaSpout<?, ?> getSpoutOperator(String queryName) {
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder(bootstrapServer + ":" + kafkaPort, topic);
        builder.setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST);  // setting the offset strategy to latest --> always start at the end offset of a stream
        builder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE);
        builder.setProp("group.id", this.topic + "-consumer");
        return new KafkaSpout<>(builder.build(), queryName, localExecution, config);
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.OperatorProperties.OPERATOR_TYPE, Constants.Operators.SPOUT);
        description.put("confEventRate", eventRate);
        description.put("numInteger", numTupleDatatypes.getFirst());
        description.put("numDouble", numTupleDatatypes.getSecond());
        description.put("numString", numTupleDatatypes.getThird());
        description.put("tupleWidthOut", numTupleDatatypes.getSum() + 2); // add timestamp value manually
        description.put(Constants.OperatorProperties.TOPIC, topic);
        return description;
    }

    public void setEventRate(double eventRate) {
        this.eventRate = eventRate;
    }

    public void setTupleWidth(Triple<Integer, Integer, Integer> tupleWidthPerDatatype) {
        this.numTupleDatatypes = tupleWidthPerDatatype;
    }
}