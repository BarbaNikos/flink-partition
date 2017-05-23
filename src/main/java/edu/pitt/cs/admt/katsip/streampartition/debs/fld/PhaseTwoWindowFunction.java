package edu.pitt.cs.admt.katsip.streampartition.debs.fld;

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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class PhaseTwoWindowFunction extends
    RichAllWindowFunction<Tuple3<Long, String, Integer>, String, TimeWindow> {
  
  public static final String numCallsAccumulatorName = "p2-fld-num-calls";
  public static final String accumulatorName = "p2-max-input-size-fld-acc";
  private Logger log = LoggerFactory.getLogger(PhaseTwoWindowFunction.class);
  private DescriptiveStatistics statistics;
  private int maxInput = -1;
  private IntMaximum maxInputAccumulator;
  private IntCounter numberOfApplyCalls;
  
  @Override
  public void open(Configuration parameters) {
    statistics = new DescriptiveStatistics();
    maxInput = -1;
    maxInputAccumulator = new IntMaximum();
    getRuntimeContext().addAccumulator(PhaseTwoWindowFunction.accumulatorName, this.maxInputAccumulator);
    numberOfApplyCalls = new IntCounter();
    getRuntimeContext().addAccumulator(PhaseTwoWindowFunction.numCallsAccumulatorName, this.numberOfApplyCalls);
  }
  
  @Override
  public void close() {
    this.maxInputAccumulator.add(maxInput);
    String msg = "fld-phase-2 object " + hashCode() + " received max input size: " + maxInput +
        ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
        statistics.getMin() + " (msec).";
    log.info(msg);
    //System.out.println(msg);
  }
  
  @Override
  public void apply(TimeWindow window, Iterable<Tuple3<Long, String, Integer>> values,
                    Collector<String> out) throws Exception {
    numberOfApplyCalls.add(1);
    DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
    TreeMap<Integer, List<String>> index = new TreeMap<>();
    int inputSize = 0;
    long start = System.currentTimeMillis();
    for (Tuple3<Long, String, Integer> event : values) {
      ++inputSize;
      if (index.containsKey(event.f2)) {
        List<String> tmp = index.get(event.f2);
        if (!tmp.contains(event.f1)) {
          tmp.add(event.f1);
          index.put(event.f2, tmp);
        }
      } else {
        List<String> tmp = new LinkedList<>();
        tmp.add(event.f1);
        index.put(event.f2, tmp);
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
    long end = System.currentTimeMillis();
    stringBuilder.append("}");
    String s = "From: " + dateFormat.format(new Date(window.getStart())) + ", To: " + dateFormat.format(new Date(window.getEnd())) +
        ", Most frequent routes: " + stringBuilder.toString() + " (window-size: " + inputSize + ").";
    out.collect(s);
    maxInput = maxInput < inputSize ? inputSize : maxInput;
    statistics.addValue(Math.abs(end - start));
  }
}
