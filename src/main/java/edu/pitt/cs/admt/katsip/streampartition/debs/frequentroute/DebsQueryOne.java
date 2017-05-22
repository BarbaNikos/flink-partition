package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import com.google.common.base.Preconditions;
import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.QueryOneAffinityPartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.QueryOneHashPartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute.QueryOneShufflePartition;
import edu.pitt.cs.admt.katsip.streampartition.debs.util.DebsCellDelegate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

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
        // Set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Phase 0: Collect input and assign timestamps
        DataStream<Tuple2<Long, String>> rideStream = env
                .readTextFile(args[0])
                .flatMap(new RichFlatMapFunction<String, Tuple2<Long, String>>() {
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
