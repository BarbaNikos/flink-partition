package edu.pitt.cs.admt.katsip.streampartition.gcm;

import com.google.common.base.Preconditions;
import edu.pitt.cs.admt.katsip.streampartition.gcm.fld.QueryTwoHashPartition;
import edu.pitt.cs.admt.katsip.streampartition.gcm.shf.QueryTwoShufflePartition;
import edu.pitt.cs.admt.katsip.streampartition.gcm.util.TaskEventDeserializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class GcmQueryTwo {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("required arguments: <path-to-rides-file> <parallelism> <partition-algo={shf,fld,ak}>");
      System.exit(1);
    }
    int parallelism = Integer.parseInt(args[1]);
    Preconditions.checkArgument(parallelism >= 1);
    Preconditions.checkArgument(args[2].equals("shf") || args[2].equals("fld") || args[2].equals("ak"));
    
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment().disableOperatorChaining();
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // Phase 0: collect input and assign timestamps
    DataStream<Tuple3<Long, Long, Float>> taskEvenStream = streamEnv.readTextFile(args[0]).map(new MapFunction<String, Tuple3<Long, Long, Float>>() {
      @Override
      public Tuple3<Long, Long, Float> map(String value) throws Exception {
        Tuple13<Long, Integer, Long, Long, Long, Integer, String, Integer, Integer, Float, Float, Float, Integer> taskEvent = TaskEventDeserializer.deSerialize(value);
        return new Tuple3<Long, Long, Float>(taskEvent.f0, taskEvent.f2, taskEvent.f9);
      }
    }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Long, Float>>() {
      @Override
      public long extractAscendingTimestamp(Tuple3<Long, Long, Float> element) {
        return element.f0;
      }
    });
    switch (args[2]) {
      case "shf":
        QueryTwoShufflePartition.submit(streamEnv, taskEvenStream, parallelism);
        break;
      case "fld":
        QueryTwoHashPartition.submit(streamEnv, taskEvenStream, parallelism);
        break;
      default:
        System.err.println("unrecognized partition algorithm provided.");
        break;
    }
  }
}
