package edu.pitt.cs.admt.katsip.streampartition;

import com.google.common.base.Preconditions;
import edu.pitt.cs.admt.katsip.streampartition.debs.DebsCellDelegate;
import edu.pitt.cs.admt.katsip.streampartition.debs.QueryOneAffinityPartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.QueryOneHashPartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.QueryOneShufflePartition;
import edu.pitt.cs.admt.katsip.streampartition.flink.NaiveAffinityCardPartitioner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/16/2017.
 */
public class DebsQueryOne {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("required arguments: <path-to-rides-file> <parallelism> <partition-algo={shf,fld,ak}>");
            System.exit(1);
        }
        int parallelism = Integer.parseInt(args[1]);
        Preconditions.checkArgument(parallelism >= 1);
        Preconditions.checkArgument(args[2].equals("shf") || args[2].equals("fld") || args[2].equals("ak"));
        List<Tuple2<Long, String>> rides;
//        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
//            DebsCellDelegate delegate = new DebsCellDelegate(DebsCellDelegate.Query.FREQUENT_ROUTE);
//            for (String line; (line = reader.readLine()) != null; ) {
//                Tuple7<String, Long, Long, String, String, Float, Float> ride = delegate.deserializeRide(line);
//                if (ride != null)
//                    rides.add(new Tuple2<>(ride.f2, ride.f3 + "-" + ride.f4));
//            }
//        }
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        batchEnv.setParallelism(parallelism);
        DataSet<Tuple2<Long, String>> rideDataset = batchEnv.readTextFile(args[0]).flatMap(new RichFlatMapFunction<String, Tuple2<Long, String>>() {

            private DebsCellDelegate delegate;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.delegate = new DebsCellDelegate(DebsCellDelegate.Query.FREQUENT_ROUTE);
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
                Tuple7<String, Long, Long, String, String, Float, Float> ride = delegate.deserializeRide(value);
                if (ride != null)
                    out.collect(new Tuple2<>(ride.f2, ride.f3 + "-" + ride.f4));
            }
        });
        rides = rideDataset.collect();
        JobExecutionResult batchJob = batchEnv.execute();
        System.out.println("Collecting the dataset took: " + (double) (batchJob.getNetRuntime(TimeUnit.MILLISECONDS) / 1000l) + " (sec).");

        // Set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().disableOperatorChaining();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Phase 0: Collect input and assign timestamps
        DataStreamSource<Tuple2<Long, String>> rideStream = env.fromCollection(rides);
        DataStream<Tuple3<Long, String, Integer>> timestampedRideStream = rideStream
                .map(new MapFunction<Tuple2<Long, String>, Tuple3<Long, String, Integer>>() {
                    @Override
                    public Tuple3<Long, String, Integer> map(Tuple2<Long, String> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, 1);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, String, Integer> event) {
                        return event.f0;
                    }
        });
        switch (args[2]) {
            case "shf":
                QueryOneShufflePartition.submit(env, timestampedRideStream, parallelism);
                break;
            case "fld":
                QueryOneHashPartition.submit(env, timestampedRideStream, parallelism);
                break;
            case "ak":
                QueryOneAffinityPartition.submit(env, timestampedRideStream, parallelism);
                break;
            default:
                System.err.println("unrecognized partition algorithm provided.");
                break;
        }
    }
}
