package edu.pitt.cs.admt.katsip.streampartition.gcm.fld;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class QueryTwoHashPartition {
    public static void submit(StreamExecutionEnvironment env, DataStream<Tuple3<Long, Long, Float>> timestampedEventStream,
                              int parallelism) throws Exception {
        // Phase 1: parallel partial aggregation
        DataStream<Tuple4<Long, Long, Float, Integer>> parallelComputation = timestampedEventStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .apply(new HashPhaseOneWindowFunction())
                .setParallelism(parallelism);
        // Phase 2: serial full aggregation
        DataStream<String> serialAggregation = parallelComputation
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .apply(new HashPhaseTwoWindowFunction()).setParallelism(1);
        //serialAggregation.print();
        JobExecutionResult executionResult = env.execute();
        int phaseOneNumCalls = executionResult.getAccumulatorResult(HashPhaseOneWindowFunction.numberOfCallsAccumulator);
        int phaseTwoNumCalls = executionResult.getAccumulatorResult(HashPhaseTwoWindowFunction.numCallsAccumulatorName);
        int phaseOneMaxInputSize = executionResult.getAccumulatorResult(HashPhaseOneWindowFunction.accumulatorName);
        int phaseTwoMaxInputSize = executionResult.getAccumulatorResult(HashPhaseTwoWindowFunction.accumulatorName);
        long jobNetRuntime = executionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        System.out.println("FLD Net runtime: " + jobNetRuntime + " (msec). Phase 1 max input size: " + phaseOneMaxInputSize +
                ", Phase 2 max input size: " + phaseTwoMaxInputSize + ", P1 number of calls: " + phaseOneNumCalls +
                ", P2 number of calls: " + phaseTwoNumCalls);
    }
}
