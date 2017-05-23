package edu.pitt.cs.admt.katsip.streampartition.debs;

import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.QueryOneHashPartition;
import edu.pitt.cs.admt.katsip.streampartition.util.SimUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
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
import org.apache.flink.util.Preconditions;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 5/22/17.
 */
public class DebsQueryOneShed {
  public static void main(String[] args) throws Exception {
    LocalDate localDate = LocalDate.now(ZoneId.of("UTC-05:00"));
    if (args.length < 5) {
      System.err.println("arguments: <path-to-file> <parallelism> <shed-prob> <RBD-p> " +
          "<output-csv>");
      System.exit(1);
    }
    int parallelism = Integer.parseInt(args[1]);
    double shedProbability = Double.parseDouble(args[2]);
    double rbdP = Double.parseDouble(args[3]);
    String outputFileName = args[4] + "_" + localDate + ".csv";
    Preconditions.checkArgument(parallelism >= 1);
    Preconditions.checkArgument(shedProbability >= 0.0 && shedProbability <= 1.0);
    Preconditions.checkArgument(rbdP > 0.0f && rbdP < 0.9f);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // Phase 0: Collect input and create timestamps
    DataStream<Tuple3<Long, String, Integer>> rideStream = env
        .readTextFile(args[0])
        .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
          @Override
          public Tuple3<Long, String, Integer> map(String s) throws Exception {
            String[] tokens = s.split(",");
            return new Tuple3<Long, String, Integer>(Long.parseLong(tokens[2]),
                tokens[3] + "." + tokens[4] + "-" + tokens[5] +
                    "." + tokens[6], 1);
          }
        })
        .assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {
              @Override
              public long extractAscendingTimestamp(Tuple3<Long, String, Integer> t) {
                return t.f0;
              }
            });
    // Phase 1: Normal and Shed Calculation
    DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> noShed = QueryOneHashPartition.submit(
        rideStream, parallelism);
    DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> withShed = QueryOneHashPartition
        .shedSubmit(rideStream, parallelism, shedProbability);
    // Phase 2: Join and compare results
    DataStream<Tuple3<Long, Double, Double>> result = noShed
        .join(withShed)
        .where(new KeySelector<Tuple2<Long, List<Tuple2<String, Integer>>>, Long>() {
          @Override
          public Long getKey(Tuple2<Long, List<Tuple2<String, Integer>>> t) throws Exception {
            return t.f0;
          }})
        .equalTo(new KeySelector<Tuple2<Long, List<Tuple2<String, Integer>>>, Long>() {
          @Override
          public Long getKey(Tuple2<Long, List<Tuple2<String, Integer>>> t) throws Exception {
            return t.f0;
          }})
        .window(TumblingEventTimeWindows.of(Time.minutes(30)))
        .apply(new JoinFunction<Tuple2<Long, List<Tuple2<String, Integer>>>,
            Tuple2<Long, List<Tuple2<String, Integer>>>, Tuple3<Long, Double, Double>>() {
          @Override
          public Tuple3<Long, Double, Double> join(
              Tuple2<Long, List<Tuple2<String, Integer>>> t1,
              Tuple2<Long, List<Tuple2<String, Integer>>> t2) throws Exception {
            List<String> t1Keys = new ArrayList<>();
            for (Tuple2<String, Integer> t : t1.f1)
              t1Keys.add(t.f0);
            List<String> t2Keys = new ArrayList<>();
            for (Tuple2<String, Integer> t : t2.f1)
              t2Keys.add(t.f0);
            return new Tuple3<>(t1.f0,
                SimUtils.RankedBiasedDistance(t1Keys, t2Keys, 0.8),
                SimUtils.JaccardSimilarity(t1Keys, t2Keys));
          }});
    result.writeAsCsv(outputFileName);
    JobExecutionResult executionResult = env.execute();
  }
}
