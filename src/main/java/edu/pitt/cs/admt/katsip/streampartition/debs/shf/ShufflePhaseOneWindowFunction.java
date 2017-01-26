package edu.pitt.cs.admt.katsip.streampartition.debs.shf;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class ShufflePhaseOneWindowFunction extends
        RichWindowFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Integer, TimeWindow> {

    private Logger log = LoggerFactory.getLogger(ShufflePhaseOneWindowFunction.class);

    private DescriptiveStatistics statistics;

    private int maxInput;

    private IntMaximum maxInputAccumulator;

    private IntCounter numberOfCalls;

    public static final String callsAccumulatorName = "p1-shf-num-calls";

    public static final String accumulatorName = "p1-max-input-size-shf-acc";

    @Override
    public void open(Configuration parameters) {
        numberOfCalls = new IntCounter();
        getRuntimeContext().addAccumulator(ShufflePhaseOneWindowFunction.callsAccumulatorName, this.numberOfCalls);
        statistics = new DescriptiveStatistics();
        maxInput = -1;
        maxInputAccumulator = new IntMaximum();
        getRuntimeContext().addAccumulator(accumulatorName, this.maxInputAccumulator);
    }

    @Override
    public void close() {
        this.maxInputAccumulator.add(maxInput);
        String msg = "shf-phase-1 object " + hashCode() + " received max input size: " + maxInput +
                ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
                statistics.getMin() + " (msec).";
        log.info(msg);
        System.out.println(msg);
    }

    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Tuple3<Long, String, Integer>> input, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
        HashMap<String, Integer> partialFrequencyIndex = new HashMap<>();
        int inputSize = 0;
        numberOfCalls.add(1);
        long start = System.currentTimeMillis();
        for (Tuple3<Long, String, Integer> t : input) {
            ++inputSize;
            if (partialFrequencyIndex.containsKey(t.f1))
                partialFrequencyIndex.put(t.f1, partialFrequencyIndex.get(t.f1) + t.f2);
            else
                partialFrequencyIndex.put(t.f1, t.f2);
        }
        for (String key : partialFrequencyIndex.keySet())
            out.collect(new Tuple3<>(window.getEnd(), key, partialFrequencyIndex.get(key)));
        maxInput = maxInput < inputSize ? inputSize : maxInput;
        long end = System.currentTimeMillis();
        statistics.addValue(Math.abs(end - start));
    }
}
