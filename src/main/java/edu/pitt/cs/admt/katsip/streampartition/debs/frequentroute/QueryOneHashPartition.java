package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.debs.fld.HashPhaseOneWindowFunction;
import edu.pitt.cs.admt.katsip.streampartition.debs.fld.HashPhaseTwoWindowFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class QueryOneHashPartition {
    public static void submit(StreamExecutionEnvironment env,
                              DataStream<Tuple3<Long, String, Integer>> timestampedRideStream,
                              int parallelism) throws Exception {
        // Phase 1: parallel partial aggregation
        DataStream<Tuple3<Long, String, Integer>> parallelComputation = timestampedRideStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new HashPhaseOneWindowFunction())
                .setParallelism(parallelism);
        // Phase 3: serial full aggregation
        DataStream<String> serialAggregation = parallelComputation
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new HashPhaseTwoWindowFunction()).setParallelism(1);
        serialAggregation.print();
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

    public static DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> submit(
            DataStream<Tuple3<Long, String, Integer>> rideStream, int parallelism) {
        DataStream<Tuple3<Long, String, Integer>> phaseOne = rideStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new HashPhaseOneWindowFunction())
                .setParallelism(parallelism);
        return phaseOne.windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new FrequentRouteSerialAggregation()).setParallelism(1);
    }

    public static DataStream<Tuple2<Long, List<Tuple2<String, Integer>>>> shedSubmit(
            DataStream<Tuple3<Long, String, Integer>> rideStream, int parallelism, double shedProbability) {
        DataStream<Tuple3<Long, String, Integer>> phaseOne = rideStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new PhaseOneShedWindowFunction(shedProbability))
                .setParallelism(parallelism);
        return phaseOne.windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new FrequentRouteSerialAggregation()).setParallelism(1);
    }
}
