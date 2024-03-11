package costream.plan.executor.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import costream.plan.executor.main.Constants;


import java.util.Collections;
import java.util.Map;

public class MongoClientProvider {
    public static MongoClient getMongoClient(Map<String, Object> config) {
        MongoCredential credential = MongoCredential.createCredential(
                Constants.Mongo.MONGO_USER,
                Constants.Mongo.MONGO_DATABASE,
                Constants.Mongo.MONGO_PASSWORD.toCharArray());

        int mongoPort;
        try {
            mongoPort = (int) config.get(Constants.MongoConstants.MONGO_PORT);
        } catch (ClassCastException e ) {
            mongoPort = (int) (long) config.get(Constants.MongoConstants.MONGO_PORT);
        }

        return new MongoClient(
                new ServerAddress((String) config.get(Constants.MongoConstants.MONGO_ADDRESS),
                        mongoPort),
                Collections.singletonList(credential));
    }
}
