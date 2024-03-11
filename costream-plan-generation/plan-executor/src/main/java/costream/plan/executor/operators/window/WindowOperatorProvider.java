package costream.plan.executor.operators.window;

import costream.plan.executor.main.Constants;
import costream.plan.executor.operators.AbstractOperatorProvider;
import costream.plan.executor.utils.RanGen;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.streams.windowing.TumblingWindows;
import org.apache.storm.streams.windowing.Window;
import org.apache.storm.topology.base.BaseWindowedBolt;


public class WindowOperatorProvider extends AbstractOperatorProvider<WindowOperator> {
    public WindowOperatorProvider() {
        this.supportedClasses.add(Integer.class);
        this.supportedClasses.add(Double.class);
        this.supportedClasses.add(String.class);

        // -- Tumbling Windows -- //
        BaseWindowedBolt.Count countWindow = getRandomCountWindow();
        operators.add(
                new WindowOperator(
                        "tumblingWindow",
                        "count",
                        TumblingWindows.of(countWindow),
                        countWindow.value,
                        null));

        BaseWindowedBolt.Duration durationWindow = getRandomDurationWindow();
        operators.add(
                new WindowOperator(
                        "tumblingWindow",
                        "duration",
                        TumblingWindows.of(durationWindow),
                        durationWindow.value,
                        null));


        // -- Sliding Windows -- //
        durationWindow = getRandomDurationWindow();
        BaseWindowedBolt.Duration slidingDurationWindow = getSlidingDurationWindow((
                int) (durationWindow.value * RanGen.randDoubleFromList(Constants.TrainingParams.WINDOW_SLIDING_RATIO)));
        operators.add(
                new WindowOperator(
                        "slidingWindow",
                        "duration",
                        SlidingWindows.of(durationWindow, slidingDurationWindow),
                        durationWindow.value,
                        slidingDurationWindow.value));

        countWindow = getRandomCountWindow();
        BaseWindowedBolt.Count slidingCountWindow = getSlidingCountWindow(
                (int) (countWindow.value * RanGen.randDoubleFromList(Constants.TrainingParams.WINDOW_SLIDING_RATIO)));
        operators.add(
                new WindowOperator(
                        "slidingWindow",
                        "count",
                        SlidingWindows.of(countWindow, slidingCountWindow),
                        countWindow.value,
                        slidingCountWindow.value));
    }

    private BaseWindowedBolt.Count getRandomCountWindow() {
        return BaseWindowedBolt.Count.of(RanGen.randIntFromList(Constants.TrainingParams.WINDOW_LENGTH));
    }

    private BaseWindowedBolt.Duration getRandomDurationWindow() {
        return BaseWindowedBolt.Duration.of(RanGen.randIntFromList(Constants.TrainingParams.WINDOW_DURATION));
    }

    private BaseWindowedBolt.Duration getSlidingDurationWindow(int duration) {
        return BaseWindowedBolt.Duration.of(duration);
    }

    private BaseWindowedBolt.Count getSlidingCountWindow(int amount) {
        return BaseWindowedBolt.Count.of(amount);
    }

    public Window<?, ?> getWindow(int windowLength, int slidingLength, String windowPolicy, String windowType) {
        if (windowPolicy.equals("duration") && windowType.equals("slidingWindow")) {
            return SlidingWindows.of(BaseWindowedBolt.Duration.of(windowLength), BaseWindowedBolt.Duration.of(slidingLength));
        } else if (windowPolicy.equals("count") && windowType.equals("slidingWindow")) {
            return SlidingWindows.of(BaseWindowedBolt.Count.of(windowLength), BaseWindowedBolt.Count.of(slidingLength));
        } else if (windowPolicy.equals("duration") && windowType.equals("tumblingWindow")) {
            return TumblingWindows.of(BaseWindowedBolt.Duration.of(windowLength));
        } else if (windowPolicy.equals("count") && windowType.equals("tumblingWindow")) {
            return TumblingWindows.of(BaseWindowedBolt.Count.of(windowLength));
        }
        throw new IllegalArgumentException("No window found with settings: " +  windowPolicy + windowPolicy);
    }
}
