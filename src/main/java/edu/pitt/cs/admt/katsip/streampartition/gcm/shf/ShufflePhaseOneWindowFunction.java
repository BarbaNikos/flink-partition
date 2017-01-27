package edu.pitt.cs.admt.katsip.streampartition.gcm.shf;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class ShufflePhaseOneWindowFunction extends RichWindowFunction<Tuple3<Long, Long, Float>, Tuple4<Long, Long, Float, Integer>, Integer, TimeWindow> {

    private Logger log = LoggerFactory.getLogger(ShufflePhaseOneWindowFunction.class);

    private DescriptiveStatistics statistics;

    private int maxInput;

    private IntMaximum maxInputAccumulator;

    private IntCounter numberOfApplyCalls;

    public static final String numberOfCallsAccumulator = "p1-shf-num-calls";

    public static final String accumulatorName = "p1-max-input-size-shf-acc";

    @Override
    public void open(Configuration parameters) {
        statistics = new DescriptiveStatistics();
        maxInput = -1;
        maxInputAccumulator = new IntMaximum();
        getRuntimeContext().addAccumulator(ShufflePhaseOneWindowFunction.accumulatorName, this.maxInputAccumulator);
        numberOfApplyCalls = new IntCounter();
        getRuntimeContext().addAccumulator(ShufflePhaseOneWindowFunction.numberOfCallsAccumulator, this.numberOfApplyCalls);
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
    public void apply(Integer integer, TimeWindow window, Iterable<Tuple3<Long, Long, Float>> input, Collector<Tuple4<Long, Long, Float, Integer>> out) throws Exception {
        HashMap<Long, Integer> partialCount = new HashMap<>();
        HashMap<Long, Float> partialSum = new HashMap<>();
        numberOfApplyCalls.add(1);
        int inputSize = 0;
        long start = System.currentTimeMillis();
        for (Tuple3<Long, Long, Float> t : input) {
            if (partialCount.containsKey(t.f1)) {
                partialCount.put(t.f1, partialCount.get(t.f1) + 1);
                partialSum.put(t.f1, partialSum.get(t.f1) + t.f2);
            } else {
                partialCount.put(t.f1, 1);
                partialSum.put(t.f1, t.f2);
            }
            ++inputSize;
        }
        long end = System.currentTimeMillis();
        for (Long jobId : partialCount.keySet()) {
            out.collect(new Tuple4<Long, Long, Float, Integer>(window.getEnd(), jobId, partialSum.get(jobId), partialCount.get(jobId)));
        }
        maxInput = maxInput < inputSize ? inputSize : maxInput;
        statistics.addValue(Math.abs(end - start));
    }
}
