package edu.pitt.cs.admt.katsip.streampartition.debs.fld;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple;
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
public class PhaseOneWindowFunction extends RichWindowFunction<Tuple3<Long, String, Integer>,
    Tuple3<Long, String, Integer>, Tuple, TimeWindow> {
  
  public static final String numberOfCallsAccumulator = "p1-fld-num-calls";
  public static final String accumulatorName = "p1-max-input-size-fld-acc";
  private Logger log = LoggerFactory.getLogger(PhaseOneWindowFunction.class);
  private DescriptiveStatistics statistics;
  private int maxInput;
  private IntMaximum maxInputAccumulator;
  private IntCounter numberOfApplyCalls;
  
  @Override
  public void open(Configuration parameters) {
    statistics = new DescriptiveStatistics();
    maxInput = -1;
    maxInputAccumulator = new IntMaximum();
    getRuntimeContext().addAccumulator(PhaseOneWindowFunction.accumulatorName, this.maxInputAccumulator);
    numberOfApplyCalls = new IntCounter();
    getRuntimeContext().addAccumulator(PhaseOneWindowFunction.numberOfCallsAccumulator, this.numberOfApplyCalls);
  }
  
  @Override
  public void close() {
    this.maxInputAccumulator.add(maxInput);
    String msg = "fld-phase-1 object " + hashCode() + " received max input size: " + maxInput +
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
      if (partialFrequencyIndex.containsKey(t.f1))
        partialFrequencyIndex.put(t.f1, partialFrequencyIndex.get(t.f1) + t.f2);
      else
        partialFrequencyIndex.put(t.f1, t.f2);
      ++inputSize;
    }
    long end = System.currentTimeMillis();
    for (String key : partialFrequencyIndex.keySet())
      out.collect(new Tuple3<>(window.getEnd(), key, partialFrequencyIndex.get(key)));
    maxInput = maxInput < inputSize ? inputSize : maxInput;
    statistics.addValue(Math.abs(end - start));
  }
}
