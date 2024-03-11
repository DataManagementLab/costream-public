package costream.plan.executor.kafka;

import costream.plan.executor.utils.Triple;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class KafkaFileReadTupleProducer extends KafkaTupleProducer {
    private String filePath;
    private Scanner input;
    private String mode;
    /**
     * Creates DataTuples on a given kafka topic
     *
     * @param queryName  Query to create topics for.
     * @param topic      Topic to run against
     * @param props      KafkaConfigs for the producers
     * @param tupleWidth Tuple width to set up for this KafkaTupleProducer
     */
    public KafkaFileReadTupleProducer(String queryName, String topic, Properties props, float throughputPerThread, Triple<Integer, Integer, Integer> tupleWidth,
                                      String mode, String filePath){
        super(queryName, topic, props, throughputPerThread, tupleWidth);
        this.filePath = filePath;
        this.mode = mode;
        resetFile();
    }

    @Override
    public String nextTuple() {
        ArrayList<Object> tuple = readNext();
        return tuple.toString();
    }

    private void resetFile() {
        Scanner input = new Scanner(filePath);
        File file = new File(input.nextLine());
        try{
            this.input = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Object> readNext() {
        String line;
        if (!input.hasNextLine()) {
            resetFile();
        }
        line = input.nextLine();

        String[] out;
        if (this.mode.startsWith("ad")) {
            out = line.split("\t");
        } else {
            out = line.split(" ");
        }

        ArrayList<Object> tupleValues = new ArrayList<>(Arrays.asList(out));
        tupleValues.add(0, queryName);
        tupleValues.add(0, String.valueOf(System.currentTimeMillis()));
        tupleValues.add(0, String.valueOf(System.currentTimeMillis()));
        return tupleValues;
    }

}
