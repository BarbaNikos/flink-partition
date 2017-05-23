package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.IntMaximum;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by Nikos R. Katsipoulakis on 5/22/17.
 */
public class FrequentRouteSerialAggregation extends
    RichAllWindowFunction<Tuple3<Long, String, Integer>, Tuple2<Long, List<Tuple2<String, Integer>>>,
        TimeWindow> {
  
  public static final String numCallsAccumulatorName = "p2-num-calls";
  public static final String accumulatorName = "p2-max-input-size-acc";
  private Logger log = LoggerFactory.getLogger(FrequentRouteSerialAggregation.class);
  private DescriptiveStatistics statistics;
  private int maxInputSize = -1;
  private IntMaximum maxInputAccumulator;
  private IntCounter numberOfApplyCalls;
  
  @Override
  public void open(Configuration parameters) {
    statistics = new DescriptiveStatistics();
    maxInputSize = -1;
    maxInputAccumulator = new IntMaximum();
    getRuntimeContext().addAccumulator(FrequentRouteSerialAggregation.accumulatorName,
        this.maxInputAccumulator);
    numberOfApplyCalls = new IntCounter();
    getRuntimeContext().addAccumulator(FrequentRouteSerialAggregation.numCallsAccumulatorName,
        this.numberOfApplyCalls);
  }
  
  @Override
  public void close() {
    this.maxInputAccumulator.add(maxInputSize);
    String message = "phase-2-id: " + hashCode() + ", max input size: " + maxInputSize +
        ", mean: " + statistics.getMean() + ", max: " + statistics.getMax() + ", min: " +
        statistics.getMin() + "(msec).";
    log.info(message);
  }
  
  @Override
  public void apply(TimeWindow window, Iterable<Tuple3<Long, String, Integer>> values,
                    Collector<Tuple2<Long, List<Tuple2<String, Integer>>>> out) throws Exception {
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
    List<Tuple2<String, Integer>> topTen = new LinkedList<>();
    for (Integer count : index.descendingKeySet()) {
      List<String> route = index.get(count);
      for (String r : route)
        topTen.add(new Tuple2<>(r, count));
      if (topTen.size() >= 10)
        break;
    }
    long end = System.currentTimeMillis();
    out.collect(new Tuple2<>(window.getEnd(), topTen));
    maxInputSize = maxInputSize < inputSize ? inputSize : maxInputSize;
    statistics.addValue(end - start);
  }
}
