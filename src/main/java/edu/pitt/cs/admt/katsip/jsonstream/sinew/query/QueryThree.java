package edu.pitt.cs.admt.katsip.jsonstream.sinew.query;

import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class QueryThree {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Requires path to tweets file and deletes file");
            System.exit(1);
        }
        // Phase 1: Parse
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setParallelism(1);
        List<Tuple2<String, String>> t1 = batchEnv.readTextFile(args[0]).map(new TweetIdStrUserLangExtractor()).collect();
        JobExecutionResult batchResult = batchEnv.getLastJobExecutionResult();
        Long t1ParseTime = (Long) batchResult.getAccumulatorResult(TweetIdStrUserLangExtractor.accumulatorName);
        long t1ParseJobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        List<Tuple2<Long, String>> d1 = batchEnv.readTextFile(args[1]).map(new DeleteIdStrUserIdExtractor()).collect();
        batchResult = batchEnv.getLastJobExecutionResult();
        Long d1ParseTime = (Long) batchResult.getAccumulatorResult(DeleteIdStrUserIdExtractor.accumulatorName);
        long d1ParseJobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        List<Tuple2<Long, String>> d2 = batchEnv.readTextFile(args[1]).map(new DeleteIdStrUserIdExtractor()).collect();
        batchResult = batchEnv.getLastJobExecutionResult();
        Long d2ParseTime = (Long) batchResult.getAccumulatorResult(DeleteIdStrUserIdExtractor.accumulatorName);
        long d2ParseJobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        // Phase 2: Process
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1).disableOperatorChaining();
        DataStream<Tuple2<Long, String>> d1Stream = streamEnv.fromCollection(d1);
        DataStream<Tuple2<Long, String>> d2Stream = streamEnv.fromCollection(d2);
        DataStream<Long> result =
                streamEnv.fromCollection(t1)
                .join(d1Stream)
                .where(new KeySelector<Tuple2<String,String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<Long, String>, String>() {
                    @Override
                    public String getKey(Tuple2<Long, String> value) throws Exception {
                        return value.f1;
                    }
                })
                .window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> join(Tuple2<String, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple2<Long, String>(second.f0, first.f1);
                    }
                }).join(d2Stream)
                        .where(new KeySelector<Tuple2<Long,String>, String>() {
                            @Override
                            public String getKey(Tuple2<Long, String> value) throws Exception {
                                return value.f1;
                            }
                        }).equalTo(new KeySelector<Tuple2<Long, String>, String>() {
                    @Override
                    public String getKey(Tuple2<Long, String> value) throws Exception {
                        return value.f1;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))).apply(new JoinFunction<Tuple2<Long,String>, Tuple2<Long,String>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> join(Tuple2<Long, String> first, Tuple2<Long, String> second) throws Exception {
                        return new Tuple2<>(first.f0, first.f1);
                    }
                }).filter(new FilterFunction<Tuple2<Long, String>>() {
                    @Override
                    public boolean filter(Tuple2<Long, String> value) throws Exception {
                        return value.f1.equals("msa");
                    }
                }).map(new MapFunction<Tuple2<Long,String>, Long>() {
                    @Override
                    public Long map(Tuple2<Long, String> value) throws Exception {
                        return value.f0;
                    }
                });
        JobExecutionResult executionResult = streamEnv.execute();
        System.out.println("tweet-T1 parse time took: " + t1ParseTime + " (msec), t1 job net runtime: " + t1ParseJobNetRuntime +
                " (msec), delete-D1 parse time: " + d1ParseTime + " (msec), d1 job net runtime: " + d1ParseJobNetRuntime +
                " (msec), delete-D1 parse time: " + d2ParseTime + " (msec), d2 job net runtime: " + d2ParseJobNetRuntime +
                " (msec), total net runtime: " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + " (msec).");
    }
}
