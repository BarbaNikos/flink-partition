package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.debs.shf.ShufflePhaseOneWindowFunction;
import edu.pitt.cs.admt.katsip.streampartition.debs.shf.ShufflePhaseTwoWindowFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class QueryOneShufflePartition {
  public static void submit(StreamExecutionEnvironment env, DataStream<Tuple3<Long, String, Integer>> timestampedRideStream, final int parallelism) throws Exception {
    // Phase 1: parallel partial aggregation
    DataStream<Tuple3<Long, String, Integer>> parallelComputation = timestampedRideStream
        .keyBy(new KeySelector<Tuple3<Long, String, Integer>, Integer>() {
          @Override
          public Integer getKey(Tuple3<Long, String, Integer> value) throws Exception {
            return (new Random()).nextInt(parallelism);
          }
        })
        .window(TumblingEventTimeWindows.of(Time.minutes(30)))
        .apply(new ShufflePhaseOneWindowFunction())
        .setParallelism(parallelism);
//        DataStream<Tuple3<Long, String, Integer>> parallelComputation = timestampedRideStream
//                .shuffle()
//                .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
//                .apply(new ShufflePhaseOneWindowFunction())
//                .setParallelism(1);
    // Phase 3: serial full aggregation
    DataStream<String> serialAggregation = parallelComputation
        .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
        .apply(new ShufflePhaseTwoWindowFunction()).setParallelism(1);
//        serialAggregation.print();
    JobExecutionResult executionResult = env.execute();
    int phaseOneApplyCalls = executionResult.getAccumulatorResult(ShufflePhaseOneWindowFunction.callsAccumulatorName);
    int phaseTwoApplyCalls = executionResult.getAccumulatorResult(ShufflePhaseTwoWindowFunction.callsAccumulatorName);
    int phaseOneMaxInputSize = executionResult.getAccumulatorResult(ShufflePhaseOneWindowFunction.accumulatorName);
    int phaseTwoMaxInputSize = executionResult.getAccumulatorResult(ShufflePhaseTwoWindowFunction.accumulatorName);
    long jobNetRuntime = executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
    System.out.println("SHF Net runtime: " + jobNetRuntime + " (msec). Phase 1 max input size: " + phaseOneMaxInputSize +
        ", Phase 2 max input size: " + phaseTwoMaxInputSize + ", P1 number of calls: " +
        phaseOneApplyCalls + ", P2 number of calls: " + phaseTwoApplyCalls);
  }
}
