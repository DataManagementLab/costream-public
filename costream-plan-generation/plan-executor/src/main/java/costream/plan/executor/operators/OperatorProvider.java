package costream.plan.executor.operators;

import costream.plan.executor.application.synthetic.SyntheticSpoutOperator;
import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.aggregation.AggregationIterableOperatorProvider;
import costream.plan.executor.operators.aggregation.AggregationOperatorProvider;
import costream.plan.executor.operators.aggregation.AggregationPairOperatorProvider;
import costream.plan.executor.operators.filter.FilterOperatorProvider;
import costream.plan.executor.operators.map.MapPairOperatorProvider;
import costream.plan.executor.utils.RanGen;
import costream.plan.executor.operators.window.WindowOperatorProvider;

/**
 * This class provides any random operator if needed. It makes use of several Subtype Operator Providers
 */
public class OperatorProvider {
    private WindowOperatorProvider windowProvider;
    private AggregationOperatorProvider aggregationProvider;
    private AggregationPairOperatorProvider aggregationPairOperatorProvider;
    private FilterOperatorProvider filterProvider;
    private MapPairOperatorProvider mapPairProvider;
    private AggregationIterableOperatorProvider aggregationIterableOperatorProvider;
    private Class<?> mapToPairClass;   // this class is the current class to map all paths on for joins (same join key assumed)
    private Class<?> aggregationClass; // this is the class that remains after aggregation

    public OperatorProvider() {
        this.reset();
    }

    /**
     * Provides a random operator of given type with a given index. Note that depending on previous operators, the output depends.
     * If an aggregation is performed on a specific class, a subsequent filter will also be only applied on that class.
     *
     * @param type  Name of the operator
     * @param index Index (=id) of the operator
     * @return random operator of given type
     */
    public AbstractOperator<?> provideOperator(String type, String index) {
        AbstractOperator<?> operator;
        switch (type) {
            case Constants.Operators.SPOUT:
                operator = new SyntheticSpoutOperator(RanGen.randIntFromList(Constants.TrainingParams.EVENT_RATES));
                break;
            case Constants.Operators.FILTER:
            case Constants.Operators.PAIR_FILTER:
                if (aggregationClass == null) {
                    operator = filterProvider.provideRandomOperator();
                } else {
                    operator = filterProvider.provideRandomOperator(aggregationClass);
                }
                break;
            case Constants.Operators.MAP_TO_PAIR:
                if (mapToPairClass == null) {
                    operator = mapPairProvider.provideRandomOperator();
                    mapToPairClass = operator.getKlass();
                } else {
                    operator = mapPairProvider.provideRandomOperator(mapToPairClass);
                }
                break;
            case Constants.Operators.AGGREGATE:
                operator = aggregationProvider.provideRandomOperator();
                aggregationClass = operator.getKlass();
                break;
            case Constants.Operators.PAIR_AGGREGATE:
                operator = aggregationPairOperatorProvider.provideRandomOperator();
                aggregationClass = operator.getKlass();
                break;
            case Constants.Operators.PAIR_ITERABLE_AGGREGATE:
                operator = aggregationIterableOperatorProvider.provideRandomOperator();
                aggregationClass = operator.getKlass();
                break;
            case Constants.Operators.WINDOW:
                operator = windowProvider.provideRandomOperator();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        operator.setId(index); // create unique identifier for operator
        return operator;
    }

    public void reset() {
        aggregationClass = null;
        mapToPairClass = null;
        windowProvider = new WindowOperatorProvider();
        aggregationProvider = new AggregationOperatorProvider();
        aggregationPairOperatorProvider = new AggregationPairOperatorProvider();
        filterProvider = new FilterOperatorProvider();
        mapPairProvider = new MapPairOperatorProvider();
        aggregationIterableOperatorProvider = new AggregationIterableOperatorProvider();
    }
}
