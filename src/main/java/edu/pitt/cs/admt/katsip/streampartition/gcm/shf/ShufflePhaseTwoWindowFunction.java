package edu.pitt.cs.admt.katsip.streampartition.gcm.shf;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class ShufflePhaseTwoWindowFunction extends RichAllWindowFunction<Tuple4<Long, Long, Float, Integer>, String, TimeWindow>{

    private Logger log = LoggerFactory.getLogger(ShufflePhaseTwoWindowFunction.class);

    private DescriptiveStatistics statistics;

    private int maxInput = -1;

    private IntMaximum maxInputAccumulator;

    private IntCounter numberOfApplyCalls;

    public static final String numCallsAccumulatorName = "p2-shf-num-calls";

    public static final String accumulatorName = "p2-max-input-size-shf-acc";

    @Override
    public void open(Configuration parameters) {
        statistics = new DescriptiveStatistics();
        maxInput = -1;
        maxInputAccumulator = new IntMaximum();
        getRuntimeContext().addAccumulator(ShufflePhaseTwoWindowFunction.accumulatorName, this.maxInputAccumulator);
        numberOfApplyCalls = new IntCounter();
        getRuntimeContext().addAccumulator(ShufflePhaseTwoWindowFunction.numCallsAccumulatorName, this.numberOfApplyCalls);
    }

    @Override
    public void close() {
        this.maxInputAccumulator.add(maxInput);
        String msg = "shf-phase-2 object " + hashCode() + " received max input size: " + maxInput +
                ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
                statistics.getMin() + " (msec).";
        log.info(msg);
        System.out.println(msg);
    }

    @Override
    public void apply(TimeWindow window, Iterable<Tuple4<Long, Long, Float, Integer>> values, Collector<String> out) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
        numberOfApplyCalls.add(1);
        HashMap<Long, List<Float>> partialSumIndex = new HashMap<>();
        HashMap<Long, List<Integer>> partialCountIndex = new HashMap<>();
        int inputSize = 0;
        long start = System.currentTimeMillis();
        for (Tuple4<Long, Long, Float, Integer> event : values) {
            ++inputSize;
            if (partialSumIndex.containsKey(event.f1)) {
                List<Float> tmpSum = partialSumIndex.get(event.f1);
                tmpSum.add(event.f2);
                partialSumIndex.put(event.f1, tmpSum);
                List<Integer> tmpCount = partialCountIndex.get(event.f1);
                tmpCount.add(event.f3);
                partialCountIndex.put(event.f1, tmpCount);
            } else {
                List<Float> tmpSum = new LinkedList<>();
                List<Integer> tmpCount = new LinkedList<>();
                tmpSum.add(event.f2);
                tmpCount.add(event.f3);
                partialSumIndex.put(event.f1, tmpSum);
                partialCountIndex.put(event.f1, tmpCount);
            }
        }
        HashMap<Long, Float> index = new HashMap<>();
        for (Long key : partialSumIndex.keySet()) {
            float sum = 0f;
            int count = 0;
            for (int i = 0; i < partialSumIndex.get(key).size(); ++i) {
                sum += partialSumIndex.get(key).get(i);
                count += partialCountIndex.get(key).get(i);
            }
            index.put(key, sum / count);
        }
        long end = System.currentTimeMillis();
        maxInput = maxInput < inputSize ? inputSize : maxInput;
        statistics.addValue(Math.abs(end - start));
        String s = "from: " + dateFormat.format(new Date(window.getStart())) + ", to: " + dateFormat.format(new Date(window.getEnd())) +
                ", total-groups: " + index.keySet();
        out.collect(s);
    }
}
