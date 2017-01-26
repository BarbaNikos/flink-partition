package edu.pitt.cs.admt.katsip.streampartition.debs.shf;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class ShufflePhaseTwoWindowFunction extends
        RichAllWindowFunction<Tuple3<Long, String, Integer>, String, TimeWindow> {

    private Logger log = LoggerFactory.getLogger(ShufflePhaseTwoWindowFunction.class);

    private DescriptiveStatistics statistics;

    private int maxInput;

    private IntMaximum maxInputAccumulator;

    private IntCounter numberOfApplyCalls;

    public static final String callsAccumulatorName = "p2-shf-num-calls";

    public static final String accumulatorName = "p2-max-input-size-shf-acc";

    @Override
    public void open(Configuration parameters) {
        statistics = new DescriptiveStatistics();
        maxInput = -1;
        maxInputAccumulator = new IntMaximum();
        getRuntimeContext().addAccumulator(ShufflePhaseTwoWindowFunction.accumulatorName, this.maxInputAccumulator);
        numberOfApplyCalls = new IntCounter();
        getRuntimeContext().addAccumulator(ShufflePhaseTwoWindowFunction.callsAccumulatorName, this.numberOfApplyCalls);
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
    public void apply(TimeWindow window, Iterable<Tuple3<Long, String, Integer>> values,
                      Collector<String> out) throws Exception {
        numberOfApplyCalls.add(1);
        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
        TreeMap<Integer, List<String>> index = new TreeMap<>();
        HashMap<String, Integer> frequencyIndex = new HashMap<>();
        int inputSize = 0;
        long start = System.currentTimeMillis();
        for (Tuple3<Long, String, Integer> t : values) {
            ++inputSize;
            if (frequencyIndex.containsKey(t.f1))
                frequencyIndex.put(t.f1, frequencyIndex.get(t.f1) + t.f2);
            else
                frequencyIndex.put(t.f1, t.f2);
        }
        for (String route : frequencyIndex.keySet()) {
            Integer finalFrequency = frequencyIndex.get(route);
            if (index.containsKey(finalFrequency)) {
                List<String> tmp = index.get(finalFrequency);
                if (!tmp.contains(route)) {
                    tmp.add(route);
                    index.put(finalFrequency, tmp);
                }
            } else {
                List<String> tmp = new LinkedList<>();
                tmp.add(route);
                index.put(finalFrequency, tmp);
            }
        }
        List<String> topTen = new LinkedList<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{");
        for (Integer count : index.descendingKeySet()) {
            List<String> route = index.get(count);
            for (String r : route) {
                topTen.add(r);
                stringBuilder.append(r + ":" + count + ",");
            }
            if (topTen.size() >= 10)
                break;
        }
        stringBuilder.append("}");
        String s = "From: " + dateFormat.format(new Date(window.getStart())) + ", To: " + dateFormat.format(new Date(window.getEnd())) +
                ", Most frequent routes: " + stringBuilder.toString();
        out.collect(s);
        maxInput = maxInput < inputSize ? inputSize : maxInput;
        long end = System.currentTimeMillis();
        statistics.addValue(Math.abs(end - start));
    }
}
