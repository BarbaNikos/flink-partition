package edu.pitt.cs.admt.katsip.streampartition.gcm.fld;

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

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class HashPhaseTwoWindowFunction extends RichAllWindowFunction<Tuple4<Long, Long, Float, Integer>, String, TimeWindow> {
  
  public static final String numCallsAccumulatorName = "p2-fld-num-calls";
  public static final String accumulatorName = "p2-max-input-size-fld-acc";
  private Logger log = LoggerFactory.getLogger(HashPhaseTwoWindowFunction.class);
  private DescriptiveStatistics statistics;
  private int maxInput = -1;
  private IntMaximum maxInputAccumulator;
  private IntCounter numberOfApplyCalls;
  
  @Override
  public void open(Configuration parameters) {
    statistics = new DescriptiveStatistics();
    maxInput = -1;
    maxInputAccumulator = new IntMaximum();
    getRuntimeContext().addAccumulator(HashPhaseTwoWindowFunction.accumulatorName, this.maxInputAccumulator);
    numberOfApplyCalls = new IntCounter();
    getRuntimeContext().addAccumulator(HashPhaseTwoWindowFunction.numCallsAccumulatorName, this.numberOfApplyCalls);
  }
  
  @Override
  public void close() {
    this.maxInputAccumulator.add(maxInput);
    String msg = "fld-phase-2 object " + hashCode() + " received max input size: " + maxInput +
        ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
        statistics.getMin() + " (msec).";
    log.info(msg);
    System.out.println(msg);
  }
  
  @Override
  public void apply(TimeWindow window, Iterable<Tuple4<Long, Long, Float, Integer>> values, Collector<String> out) throws Exception {
    DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
    numberOfApplyCalls.add(1);
    HashMap<Long, Float> index = new HashMap<>();
    int inputSize = 0;
    long start = System.currentTimeMillis();
    for (Tuple4<Long, Long, Float, Integer> event : values) {
      ++inputSize;
      index.put(event.f1, event.f2 / event.f3);
    }
    long end = System.currentTimeMillis();
    maxInput = maxInput < inputSize ? inputSize : maxInput;
    statistics.addValue(Math.abs(end - start));
    String s = "from: " + dateFormat.format(new Date(window.getStart())) + ", to: " + dateFormat.format(new Date(window.getEnd())) +
        ", total-groups: " + index.keySet();
    out.collect(s);
  }
}
