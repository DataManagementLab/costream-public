package costream.plan.executor.main;

import org.apache.storm.streams.Pair;

public interface Constants {
    int NUM_SINKS = 5;
    int DEFAULT_SWAP_SIZE = 3000; // in MB

    /**
     * Specify placement mode for randomly generated queries. Placement can be either hierarchical or completely random
     */
    String PLACEMENT_MODE = "hierarchical";

    interface QueryLabels {
        String PRODUCER_INTERVAL = "producer-interval";
        String INGESTION_INTERVAL = "ingestion-interval";
        String THROUGHPUT = "throughput";
        String TPT_MEAN = "throughput-mean";
        String E2E_MEAN = "e2e-mean";
        String PROC_MEAN = "proc-mean";
        String E2ELATENCY = "e2e-latency";
        String PROCLATENCY = "proc-latency";
        String COUNTER = "counter";
    }

    interface QueryProperties {
        String JVM_SIZE = "jvmSize";
        String QUERY = "query";
        String CLUSTER = "cluster";
        String MODE = "mode";
        String DURATION = "duration";
        String EVENT_RATE = "eventRate";
        String WINDOW_LENGTH = "windowLength";
        String TUPLE_WIDTH = "tupleWidth";
        String COMMON_RAM = "common-ram";
        String COMMON_CPU = "common-cpu";
        String COMMON_BANDWIDTH = "common-bandwidth";
        String COMMON_LATENCY = "common-latency";
    }

    interface HostProperties {
        String RAM = "ram";
        String CPU = "cpu";
        String BANDWIDTH = "bandwidth";
        String RAMSWAP = "ramswap";
        String LATENCY = "latency";
    }

    interface OperatorProperties {
        String OPERATOR_TYPE = "operatorType";
        String COMPONENT = "component";

        String TOPIC = "topic";
        String CONSUMER_POSITION = "consumer";
        String PRODUCER_POSITION = "producer";
        String OFFSET = "offset";
        String KAFKA_DURATION = "kafkaDuration";
    }

    interface QueryType {
        String RANDOM = "random";
        String RANDOM_FIXED_HW = "random-fixed-hw";
        interface FIXED {
            String LINEAR = "linear";
            String LINEAR_AGG_COUNT = "linear-agg-count";
            String LINEAR_AGG_DURATION = "linear-agg-duration";
            String TWO_WAY_JOIN = "two-way-join";
            String THREE_WAY_JOIN = "three-way-join";
            String TWO_FILTER_CHAIN = "two-filter-chain";
            String THREE_FILTER_CHAIN = "three-filter-chain";
            String FOUR_FILTER_CHAIN = "four-filter-chain";

        }
        interface BENCHMARK {
            String ADVERTISEMENT = "advertisement";
            String SPIKE_DETECTION = "spike-detection";
            String SMART_GRID = "smart-grid";
            String SMART_GRID_LOCAL = "smart-grid-local";
            String SMART_GRID_GLOBAL = "smart-grid-global";
        }
        String[] ALL = {
                FIXED.LINEAR,
                FIXED.LINEAR_AGG_COUNT,
                FIXED.LINEAR_AGG_DURATION,
                FIXED.TWO_WAY_JOIN,
                FIXED.THREE_WAY_JOIN,
                FIXED.TWO_FILTER_CHAIN,
                FIXED.THREE_FILTER_CHAIN,
                FIXED.FOUR_FILTER_CHAIN,
                BENCHMARK.ADVERTISEMENT,
                BENCHMARK.SPIKE_DETECTION,
                BENCHMARK.SMART_GRID,
                BENCHMARK.SMART_GRID_LOCAL,
                BENCHMARK.SMART_GRID_GLOBAL
        };
    }


    interface Kafka {
        int NUM_THREADS = 5;
        String TOPIC0 = "source0";
        String TOPIC1 = "source1";
        String TOPIC2 = "source2";
        String[] TOPICS = {TOPIC0, TOPIC1, TOPIC2};
    }


    enum Features {
        operatorType,
        id,
        dataType,
        filterFunction,
        filterClass,
        literal,
        mapKey,
        windowType,
        windowPolicy,
        windowLength,
        slidingLength,
        mapFunction,
        joinKeyClass,
        aggFunction,
        aggClass,
        groupByClass
    }

    interface MongoConstants {
        String MONGO_ADDRESS = "mongo.address";
        String MONGO_PORT = "mongo.port";
    }

    interface Mongo {
        String MONGO_DATABASE = "dsps";
        String MONGO_USER = "dsps_mongo";
        String MONGO_PASSWORD = "s3cure!";
    }

    interface Operators {
        String SPOUT = "spout";
        String AGGREGATE = "aggregation";
        String WINDOW_AGGREGATE = "windowedAggregation";
        String PAIR_AGGREGATE = "pairAggregation";
        String PAIR_ITERABLE_AGGREGATE = "pairIterableAggregation";
        String MAP_TO_PAIR = "mapToPair";
        String WINDOW = "window";
        String FILTER = "filter";
        String PAIR_FILTER = "pairFilter";
        String JOIN = "join";
        String SINK = "sink";
        String HOST = "host";
    }

    interface TrainingParams {
        // ----------- QUERY PARAMS ---------------------
        /**
         * Tuples per Second
         */
        int[] EVENT_RATES = new int[]{100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600};
        int[] TWO_WAY_JOIN_RATES = new int[]{50, 100, 250, 500, 750, 1000, 1250, 1500, 1750, 2000};
        int[] THREE_WAY_JOIN_RATES = new int[]{20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
        /**
         * Specify the range, how often each of [string, integer, double] can occur in a tuple
         */
        Pair<Integer, Integer> INTEGER_RANGE = Pair.of(3, 10);
        Pair<Integer, Integer> STRING_RANGE = Pair.of(3, 10);
        Pair<Integer, Integer> DOUBLE_RANGE = Pair.of(3, 10);
        /**
         * Fixed value range for integers inside a tuple
         */
        Pair<Integer, Integer> INTEGER_VALUE_RANGE = Pair.of(-10, 10);
        /**
         * Fixed value range for double inside a tuple
         */
        Pair<Double, Double> DOUBLE_VALUE_RANGE = Pair.of(-0.05, 0.05);
        /**
         * Fixed value range for strings inside a tuple
         */
        int STRING_LENGTH = 1;

        // ----------- HARDWARE PARAMS ---------------------
        /**
         * Possible RAM_SIZES in MB
         */
        int[] SMALL_RAM_SIZES = new int[]{1000, 2000, 4000};
        int[] MEDIUM_RAM_SIZES = new int[]{4000, 8000, 16000};
        int[] LARGE_RAM_SIZES = new int[]{8000, 16000, 24000, 32000};
        /**
         * Possible CPU_SIZES in percent of a core
         */
        int[] SMALL_CPU_SIZES = new int[]{50, 100, 200, 300};
        int[] MEDIUM_CPU_SIZES = new int[]{300, 400, 500, 600};
        int[] LARGE_CPU_SIZES = new int[]{600, 700, 800};
        /**
         * Possible bandwidths in MBit/s
         */
        int[] SMALL_BANDWIDTHS = new int[]{25, 50, 100, 200};
        int[] MEDIUM_BANDWIDTHS = new int[]{200, 400, 800, 1600};
        int[] LARGE_BANDWIDTHS = new int[]{1600, 3200, 6400, 10000};
        /**
         * Possible latencies in ms
         */
        int[] SMALL_LATENCIES = new int[]{40, 80, 160};
        int[] MEDIUM_LATENCIES = new int[]{10, 20, 40};
        int[] LARGE_LATENCIES = new int[]{1, 2, 5, 10};

        // These values were used for the interpolation study
        /*
        int[] SMALL_RAM_SIZES = new int[]{1500, 3000, 6000};
        int[] MEDIUM_RAM_SIZES = new int[]{6000, 12000, 20000};
        int[] LARGE_RAM_SIZES = new int[]{20000, 28000};
        int[] SMALL_CPU_SIZES = new int[]{75, 150, 250, 350};
        int[] MEDIUM_CPU_SIZES = new int[]{350, 450, 550, 650};
        int[] LARGE_CPU_SIZES = new int[]{650, 750, 850};

        int[] SMALL_BANDWIDTHS = new int[]{35, 70, 150, 250};
        int[] MEDIUM_BANDWIDTHS = new int[]{250, 550, 1200, 1900};
        int[] LARGE_BANDWIDTHS = new int[]{1900, 4800, 8000};
        int[] SMALL_LATENCIES = new int[]{30, 60, 120};
        int[] MEDIUM_LATENCIES = new int[]{15, 30, 60};
        int[] LARGE_LATENCIES = new int[]{3, 7, 15};
         */

        // ------------- QUERY PARAMS ------------------------

        int[] WINDOW_DURATION = new int[]{250, 500, 1000, 2000, 4000, 8000, 16000};
        /**
         * Possible window length for count-based windows in counts
         */
        int[] WINDOW_LENGTH = new int[]{5, 10, 20, 40, 80, 160, 320, 640};
        /**
         * Ratio of window slide against window length for sliding windows (count & duration based)
         */
        double[] WINDOW_SLIDING_RATIO = new double[]{0.3, 0.4, 0.5, 0.6, 0.7};
    }
}
