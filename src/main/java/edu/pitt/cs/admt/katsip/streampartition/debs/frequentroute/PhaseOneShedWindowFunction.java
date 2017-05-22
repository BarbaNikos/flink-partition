package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.debs.fld.HashPhaseOneWindowFunction;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Nikos R. Katsipoulakis on 5/22/17.
 */
public class PhaseOneShedWindowFunction extends RichWindowFunction<Tuple3<Long, String, Integer>,
        Tuple3<Long, String, Integer>, Tuple, TimeWindow> {

    private Logger log = LoggerFactory.getLogger(PhaseOneShedWindowFunction.class);

    private double shedProbability;

    private DescriptiveStatistics statistics;

    private int maxInput;

    private IntMaximum maxInputAccumulator;

    private IntCounter numberOfApplyCalls;

    public static final String numberOfCallsAccumulator = "p1-num-calls";

    public static final String accumulatorName = "p1-max-input-size-acc";

    public PhaseOneShedWindowFunction(double shedProbability) {
        Preconditions.checkArgument(shedProbability >= 0.0 && shedProbability <= 1.0);
        this.shedProbability = shedProbability;
    }

    @Override
    public void open(Configuration parameters) {
        statistics = new DescriptiveStatistics();
        maxInput = -1;
        maxInputAccumulator = new IntMaximum();
        getRuntimeContext().addAccumulator(HashPhaseOneWindowFunction.accumulatorName, this.maxInputAccumulator);
        numberOfApplyCalls = new IntCounter();
        getRuntimeContext().addAccumulator(HashPhaseOneWindowFunction.numberOfCallsAccumulator, this.numberOfApplyCalls);
    }

    @Override
    public void close() {
        this.maxInputAccumulator.add(maxInput);
        String msg = "phase-1 object:" + hashCode() + ", received max input size: " + maxInput +
                ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
                statistics.getMin() + " (msec).";
        log.info(msg);
        System.out.println(msg);
    }

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, Integer>> input,
                      Collector<Tuple3<Long, String, Integer>> out) throws Exception {
        HashMap<String, Integer> partialFrequencyIndex = new HashMap<>();
        numberOfApplyCalls.add(1);
        int inputSize = 0;
        long start = System.currentTimeMillis();
        for (Tuple3<Long, String, Integer> t : input) {
            double outcome = ThreadLocalRandom.current().nextDouble(0.0, 1.0001);
            if (outcome < shedProbability)
                continue;
            if (partialFrequencyIndex.containsKey(t.f1))
                partialFrequencyIndex.put(t.f1, partialFrequencyIndex.get(t.f1) + t.f2);
            else
                partialFrequencyIndex.put(t.f1, t.f2);
            ++inputSize;
        }
//        if (partialFrequencyIndex.isEmpty()) {
//            Iterator<Tuple3<Long, String, Integer>> it = input.iterator();
//            if (it.hasNext()) {
//                Tuple3<Long, String, Integer> t = it.next();
//                partialFrequencyIndex.put(t.f1, t.f2);
//            }
//        }
        long end = System.currentTimeMillis();
        for (String key : partialFrequencyIndex.keySet())
            out.collect(new Tuple3<>(window.getEnd(), key, partialFrequencyIndex.get(key)));
        maxInput = maxInput < inputSize ? inputSize : maxInput;
        statistics.addValue(Math.abs(end - start));
    }
}
