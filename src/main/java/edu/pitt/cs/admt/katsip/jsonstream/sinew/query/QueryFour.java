package edu.pitt.cs.admt.katsip.jsonstream.sinew.query;

import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.TweetInReplytoScreenNameExtractor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class QueryFour {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Requires path to tweets file");
            System.exit(1);
        }
        // Phase 1: Parse
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setParallelism(1);
        List<Tuple2<String, String>> t1 = batchEnv.readTextFile(args[0]).map(new TweetInReplytoScreenNameExtractor()).collect();
        JobExecutionResult batchResult = batchEnv.getLastJobExecutionResult();
        Long t1ParseTime = (Long) batchResult.getAccumulatorResult(TweetInReplytoScreenNameExtractor.accumulatorName);
        long t1JobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        List<Tuple2<String, String>> t2 = batchEnv.readTextFile(args[0]).map(new TweetInReplytoScreenNameExtractor()).collect();
        batchResult = batchEnv.getLastJobExecutionResult();
        Long t2ParseTime = (Long) batchResult.getAccumulatorResult(TweetInReplytoScreenNameExtractor.accumulatorName);
        long t2JobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        List<Tuple2<String, String>> t3 = batchEnv.readTextFile(args[0]).map(new TweetInReplytoScreenNameExtractor()).collect();
        batchResult = batchEnv.getLastJobExecutionResult();
        Long t3ParseTime = (Long) batchResult.getAccumulatorResult(TweetInReplytoScreenNameExtractor.accumulatorName);
        long t3JobNetRuntime = batchResult.getNetRuntime(TimeUnit.MILLISECONDS);
        // Phase 2: Process
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1).disableOperatorChaining();
        DataStream<Tuple2<String, String>> t2Stream = streamEnv.fromCollection(t2);
        DataStream<Tuple2<String, String>> t3Stream = streamEnv.fromCollection(t3);
        DataStream<Tuple2<String, String>> result = streamEnv.fromCollection(t1)
                .join(t3Stream)
                .where(new KeySelector<Tuple2<String,String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f1;
            }
        })
                .equalTo(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f1;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                .apply(new JoinFunction<Tuple2<String,String>, Tuple2<String,String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
                        if (second.f0 == null)
                            return new Tuple2<>(first.f1, "");
                        return new Tuple2<>(first.f1, second.f0);
                    }
                })
                .join(t2Stream)
                .where(new KeySelector<Tuple2<String,String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<String, String> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<String, String> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                .apply(new JoinFunction<Tuple2<String,String>, Tuple2<String,String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
                        return new Tuple2<String, String>(first.f0, second.f1);
                    }
                });
        JobExecutionResult executionResult = streamEnv.execute();
        System.out.println("tweet-T1 parse time: " + t1ParseTime + " (msec), t1 job net runtime: " + t1JobNetRuntime +
                " (msec), tweet-T2 parse time: " + t2ParseTime + " (msec), t2 job net runtime: " + t2JobNetRuntime +
                " (msec), T3 parse time: " + t3ParseTime + " (msec), t3 job net runtime: " + t3JobNetRuntime +
                " (msec), total net runtime: " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + " (msec).");
    }
}
