class CLI:
    TEST = "test"
    TRAIN = "train"


class Operators:
    JOIN = "join"
    SINK = "sink"
    SPOUT = "spout"


class LABEL:
    FAIL = "failing"
    ELAT = "e2e-mean"
    PLAT = "proc-mean"
    TPT = "throughput-mean"
    BACKPRESSURE = "offset"
    COUNTER = "counter"


class QUERY:
    TYPE = "type"


class OPERATOR:
    TYPE = "operatorType"


class Feat:
    # Spout related
    EVENT_RATE = "confEventRate"
    NUM_DOUBLE = "numDouble"
    NUM_INTEGER = "numInteger"
    NUM_STRING = "numString"

    # Data characteristic related
    TUPLE_W_IN = "tupleWidthIn"
    TUPLE_W_OUT = "tupleWidthOut"
    SELECTIVITY = "realSelectivity"

    # Placement related
    INSTANCE_SIZE = "instanceSize"

    # Join related
    JOIN_CLASS = "joinKeyClass"

    # Window related
    WINDOW_POLICY = "windowPolicy"
    WINDOW_TYPE = "windowType"
    SLIDING_LENGTH = "slidingLength"
    WINDOW_LENGTH = "windowLength"

    # Aggregation related
    AGG_FUNCTION = "aggFunction"
    AGG_CLASS = "aggClass"
    GROUP_BY_CLASS = "groupByClass"

    # Filter related
    FILTER_FUNCTION = "filterFunction"
    FILTER_CLASS = "filterClass"

    # Hardware related
    CPU = "cpu"
    RAM = "ram"
    BANDWIDTH = "bandwidth"
    LATENCY = "latency"


class Info:
    # these are not considered as features, and thus we do also not create feature statistics
    INPUT_RATE = "inputRate"
    OUTPUT_RATE = "outputRate"
    INPUT_COUNTER = "inputCounter"
    OUTPUT_COUNTER = "outputCounter"

    OP_TYPE = "operatorType"
    COMPONENT = "component"
    GROUP = "grouping"
    HOST = "host"
    ID = "id"
    _ID = "_id"
    LITERAL = "literal"
    DURATION = "duration"
    QUERY = "query"
    PORT = "port"
    RAMSWAP = "ramswap"
    SCORE = "score"
    COUNTER = "counter"
    CATEGORY = "category"

    # Kafka related properties
    INGESTION_INTERVAL = "ingestion-interval"
    PRODUCER_INTERVAL = "producer-interval"
    KAFKA_DURATION = "kafkaDuration"
    TOPIC = "topic"


class FULL_FEATURIZATION:
    # Shared characteristics
    DATA_CHARACTERISTICS = [Feat.TUPLE_W_OUT,
                            Feat.TUPLE_W_IN,
                            Feat.SELECTIVITY]

    WINDOW_CHARACTERISTICS = [Feat.WINDOW_LENGTH,
                              Feat.SLIDING_LENGTH,
                              Feat.WINDOW_TYPE,
                              Feat.WINDOW_POLICY]

    # Node type features
    HOST_FEATURES = [Feat.CPU,
                     Feat.RAM,
                     Feat.BANDWIDTH,
                     Feat.LATENCY]

    SPOUT_FEATURES = [Feat.NUM_STRING,
                      Feat.NUM_DOUBLE,
                      Feat.NUM_INTEGER,
                      Feat.EVENT_RATE,
                      Feat.TUPLE_W_OUT]

    FILTER_FEATURES = [Feat.FILTER_FUNCTION,
                       Feat.FILTER_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_JOIN_FEATURES = [Feat.JOIN_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    AGGREGATION_FEATURES = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_AGGREGATION = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    # Introduce dummy feature for sink that is always 1, otherwise sinks would have no features
    SINK = ["dummy"]

    ALL = set(
        DATA_CHARACTERISTICS + WINDOW_CHARACTERISTICS + HOST_FEATURES + SPOUT_FEATURES + FILTER_FEATURES + WINDOWED_JOIN_FEATURES + AGGREGATION_FEATURES + WINDOWED_AGGREGATION + SINK)


class NO_SEL_FEATURIZATION:
    # Shared characteristics
    DATA_CHARACTERISTICS = [Feat.TUPLE_W_OUT,
                            Feat.TUPLE_W_IN]

    WINDOW_CHARACTERISTICS = [Feat.WINDOW_LENGTH,
                              Feat.SLIDING_LENGTH,
                              Feat.WINDOW_TYPE,
                              Feat.WINDOW_POLICY]

    # Node type features
    HOST_FEATURES = [Feat.CPU,
                     Feat.RAM,
                     Feat.BANDWIDTH,
                     Feat.LATENCY]

    SPOUT_FEATURES = [Feat.NUM_STRING,
                      Feat.NUM_DOUBLE,
                      Feat.NUM_INTEGER,
                      Feat.EVENT_RATE,
                      Feat.TUPLE_W_OUT]

    FILTER_FEATURES = [Feat.FILTER_FUNCTION,
                       Feat.FILTER_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_JOIN_FEATURES = [Feat.JOIN_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    AGGREGATION_FEATURES = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_AGGREGATION = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    # Introduce dummy feature for sink that is always 1, otherwise sinks would have no features
    SINK = ["dummy"]  #
    ALL = set(
        DATA_CHARACTERISTICS + WINDOW_CHARACTERISTICS + HOST_FEATURES + SPOUT_FEATURES + FILTER_FEATURES + WINDOWED_JOIN_FEATURES + AGGREGATION_FEATURES + WINDOWED_AGGREGATION + SINK)


class ONLY_OPERATORS_FEAT:
    DATA_CHARACTERISTICS = [Feat.TUPLE_W_OUT,
                            Feat.TUPLE_W_IN]

    WINDOW_CHARACTERISTICS = [Feat.WINDOW_LENGTH,
                              Feat.SLIDING_LENGTH,
                              Feat.WINDOW_TYPE,
                              Feat.WINDOW_POLICY]

    SPOUT_FEATURES = [Feat.NUM_STRING,
                      Feat.NUM_DOUBLE,
                      Feat.NUM_INTEGER,
                      Feat.EVENT_RATE,
                      Feat.TUPLE_W_OUT]

    FILTER_FEATURES = [Feat.FILTER_FUNCTION,
                       Feat.FILTER_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_JOIN_FEATURES = [Feat.JOIN_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    AGGREGATION_FEATURES = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_AGGREGATION = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    # Introduce dummy feature for sink that is always 1, otherwise sinks would have no features
    SINK = ["dummy"]  #
    ALL = set(
        DATA_CHARACTERISTICS + WINDOW_CHARACTERISTICS + SPOUT_FEATURES + FILTER_FEATURES + WINDOWED_JOIN_FEATURES + AGGREGATION_FEATURES + WINDOWED_AGGREGATION + SINK)


class EMPTY_HOSTS:
    # Shared characteristics
    DATA_CHARACTERISTICS = [Feat.TUPLE_W_OUT,
                            Feat.TUPLE_W_IN]

    WINDOW_CHARACTERISTICS = [Feat.WINDOW_LENGTH,
                              Feat.SLIDING_LENGTH,
                              Feat.WINDOW_TYPE,
                              Feat.WINDOW_POLICY]

    # Node type features
    HOST_FEATURES = ["dummy"]

    SPOUT_FEATURES = [Feat.NUM_STRING,
                      Feat.NUM_DOUBLE,
                      Feat.NUM_INTEGER,
                      Feat.EVENT_RATE,
                      Feat.TUPLE_W_OUT]

    FILTER_FEATURES = [Feat.FILTER_FUNCTION,
                       Feat.FILTER_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_JOIN_FEATURES = [Feat.JOIN_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    AGGREGATION_FEATURES = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + DATA_CHARACTERISTICS

    WINDOWED_AGGREGATION = [Feat.AGG_FUNCTION,
                            Feat.AGG_CLASS,
                            Feat.GROUP_BY_CLASS] + WINDOW_CHARACTERISTICS + DATA_CHARACTERISTICS

    # Introduce dummy feature for sink that is always 1, otherwise sinks would have no features
    SINK = ["dummy"]  #
    ALL = set(
        DATA_CHARACTERISTICS + WINDOW_CHARACTERISTICS + HOST_FEATURES + SPOUT_FEATURES + FILTER_FEATURES + WINDOWED_JOIN_FEATURES + AGGREGATION_FEATURES + WINDOWED_AGGREGATION + SINK)
