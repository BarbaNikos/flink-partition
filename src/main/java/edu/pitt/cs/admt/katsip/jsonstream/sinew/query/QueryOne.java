package edu.pitt.cs.admt.katsip.jsonstream.sinew.query;

import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.FilterDistinctUserIdFunction;
import edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor.TweetUserIdExtractor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryOne {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Requires path to tweets file.");
            System.exit(1);
        }
        // Phase 1: Parse
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<String> tweets = env.readTextFile(args[0]);
        DataSet<Long> deserializedUserIdSet = tweets.map(new TweetUserIdExtractor());
        List<Long> userIds = deserializedUserIdSet.collect();
        JobExecutionResult batchExecutionResult = env.getLastJobExecutionResult();
        long parseNetRuntime = batchExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        Long parseTime = (Long) batchExecutionResult.getAccumulatorResult(TweetUserIdExtractor.accumulatorName);
        // Phase 2: Execute
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(1).disableOperatorChaining();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1).disableOperatorChaining();
        DataStream<Long> result = streamEnv.fromCollection(userIds).filter(new FilterDistinctUserIdFunction());
        //result.print();
        JobExecutionResult streamExecutionResult = streamEnv.execute();
        System.out.println("parse time took: " + parseTime + " (msec), parse-job net-runtime: " + parseNetRuntime +
                " (msec), total net runtime: " + streamExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS) + " (msec).");
    }
}