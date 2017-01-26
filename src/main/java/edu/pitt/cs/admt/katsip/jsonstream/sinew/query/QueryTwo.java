package edu.pitt.cs.admt.katsip.jsonstream.sinew.query;

import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.TweetUserIdExtractor;
import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.TweetUserIdRetweetCountExtractor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class QueryTwo {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Requires path to tweets file.");
            System.exit(1);
        }
        // Phase 1: Parse
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setParallelism(1);
        DataSet<Tuple2<Long, Integer>> deserializedUserIdRetweetCountSet = batchEnv.readTextFile(args[0])
                .map(new TweetUserIdRetweetCountExtractor());
        List<Tuple2<Long, Integer>> userIdAndRetweetCount = deserializedUserIdRetweetCountSet.collect();
        JobExecutionResult batchExecutionResult = batchEnv.getLastJobExecutionResult();
        long parseJobNetRuntime = batchExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        Long parseTime = (Long) batchExecutionResult.getAccumulatorResult(TweetUserIdRetweetCountExtractor.accumulatorName);
        // Phase 2: Process
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1).disableOperatorChaining();
        SingleOutputStreamOperator<Tuple2<Long, Integer>> result = streamEnv
                .fromCollection(userIdAndRetweetCount)
                .keyBy(0)
                .sum(1);
        //result.print();
        JobExecutionResult executionResult = streamEnv.execute();
        System.out.println("parse time took: " + parseTime + " (msec), parse-job net runtime: " + parseJobNetRuntime +
                " (msec), total net runtime: " + executionResult.getNetRuntime(TimeUnit.MILLISECONDS) + " (msec).");
    }
}
