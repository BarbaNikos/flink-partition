package edu.pitt.cs.admt.katsip.streampartition.debs;

import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.QueryOneHashPartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.ShedErrorFunction;
import edu.pitt.cs.admt.katsip.streampartition.util.SimUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Nikos R. Katsipoulakis on 5/22/17.
 */
public class DebsQueryOneShed {
  public static void main(String[] args) throws Exception {
    LocalDate localDate = LocalDate.now(ZoneId.of("UTC-05:00"));
    if (args.length < 6) {
      System.err.println("arguments: <path-to-file> <parallelism> <shed-prob> <RBD-p> " +
          "<output-csv> <iterations>");
      System.exit(1);
    }
    final int parallelism = Integer.parseInt(args[1]);
    final double shedProbability = Double.parseDouble(args[2]);
    final double rbdP = Double.parseDouble(args[3]);
    final String outputFileName = args[4] + "_" + localDate;
    final int iterations = Integer.parseInt(args[5]);
    Preconditions.checkArgument(parallelism >= 1);
    Preconditions.checkArgument(shedProbability >= 0.0 && shedProbability <= 1.0);
    Preconditions.checkArgument(rbdP > 0.0f && rbdP < 1.0f);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // Phase 0: Collect input and create timestamps
    DataStream<Tuple3<Long, String, Integer>> rideStream = env
        .readTextFile(args[0])
        .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
          @Override
          public Tuple3<Long, String, Integer> map(String s) throws Exception {
            String[] tokens = s.split(",");
            return new Tuple3<Long, String, Integer>(Long.parseLong(tokens[1]),
                tokens[2] + "-" + tokens[3], 1);
          }
        })
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {
              @Override
              public long extractAscendingTimestamp(Tuple3<Long, String, Integer> t) {
                return t.f0;
              }
            });
    env.execute();
    TreeMap<Integer, Integer> jaccardHistogram = new TreeMap<>();
    TreeMap<Integer, Integer> rbdHistogram = new TreeMap<>();
    int windows = 0;
    for (int i = 0; i < iterations; ++i) {
      // Phase 1: Normal and Shed Calculation
      DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> noShed = QueryOneHashPartition
          .submit(rideStream, parallelism);
      DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> withShed = QueryOneHashPartition
          .shedSubmit(rideStream, parallelism, shedProbability);
      // Phase 2: Join and compare results
      DataStream<Tuple3<Long, Double, Double>> result = noShed
          .join(withShed)
          .where(new KeySelector<Tuple2<Long, List<Tuple2<String, Integer>>>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, List<Tuple2<String, Integer>>> t) throws Exception {
              return t.f0;
            }
          })
          .equalTo(new KeySelector<Tuple2<Long, List<Tuple2<String, Integer>>>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, List<Tuple2<String, Integer>>> t) throws Exception {
              return t.f0;
            }
          })
          .window(TumblingEventTimeWindows.of(Time.minutes(30)))
          .apply(new ShedErrorFunction(rbdP));
      JobExecutionResult executionResult = env.execute();
      windows = executionResult.getAccumulatorResult(ShedErrorFunction.NumberOfWindowsAccumulator);
      TreeMap<Integer, Integer> jaccardHist = executionResult
          .getAccumulatorResult(ShedErrorFunction.JaccardHistogramAccumulator);
      jaccardHistogram = SimUtils.MergeTreeMap(jaccardHistogram, jaccardHist);
      TreeMap<Integer, Integer> rbdHist = executionResult
          .getAccumulatorResult(ShedErrorFunction.RbdHistogramAccumulator);
      rbdHistogram = SimUtils.MergeTreeMap(rbdHistogram, rbdHist);
      if (i >= (iterations - 1))
        result.writeAsText(outputFileName);
    }
    // Produce Descriptive Statistics for each value
    DescriptiveStatistics jaccardStatistics = SimUtils.GatherHistogramAccumulator(jaccardHistogram,
        1000000);
    DescriptiveStatistics rbdStatistics = SimUtils.GatherHistogramAccumulator(rbdHistogram,
        1000000);
    System.out.println("Number of windows: " + windows);
    System.out.println("JACCARD - Mean: " + jaccardStatistics.getMean() + ", Max: " +
        jaccardStatistics.getMax() + ", Min: " + jaccardStatistics.getMin());
    System.out.println("RBD - Mean: " + rbdStatistics.getMean() + ", Max: " + rbdStatistics
        .getMax() + ", Min: " + rbdStatistics.getMin());
  }
}
