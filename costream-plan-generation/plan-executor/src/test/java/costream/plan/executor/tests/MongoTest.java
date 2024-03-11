package costream.plan.executor.tests;

import org.apache.storm.shade.org.json.simple.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


public class MongoTest {
    @Test
    public void test() {
        //StreamBuilder builder = new StreamBuilder();
        //Stream<Tuple> stream = builder.newStream(new SyntheticSpoutOperator(100).getSpoutOperator("test"));
        //String url = "mongodb://192.168.12.44/dsps";
        //String collectionName = "query-labels";
        //MongoMapper mapper = new SimpleMongoMapper().withFields("Integer-0", "Integer-1");
        // MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName, mapper);
        //Triple<Integer, Integer, Integer> values = new Triple<>(1,2,3);
        //stream.to(insertBolt);
        //LocalCluster client = new LocalCluster();
        // client.submitTopology("test", new Config(), builder.build());
    }

    @Test
    public void test2() {
        //MongoClient mongoClient = new MongoClient("192.168.12.44", 27017);
        // mongoClient.getDatabase("dsps");
        //mongoClient.getDatabaseNames().forEach(System.out::println);
    }

    @Test
    public void testSerializability() throws IOException {
        JSONObject obj = new JSONObject();
        new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(obj);
    }
}

