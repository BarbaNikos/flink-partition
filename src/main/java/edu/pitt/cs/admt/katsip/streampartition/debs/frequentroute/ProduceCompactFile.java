package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.debs.util.DebsCellDelegate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Created by Nikos R. Katsipoulakis on 5/23/17.
 */
public class ProduceCompactFile {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("arguments: <input-rides.csv> <output-rides.csv>");
      System.exit(1);
    }
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    DataSet<Tuple3<Long, String, String>> compactRides = env
        .readTextFile(args[0])
        .flatMap(new RichFlatMapFunction<String, Tuple3<Long, String, String>>() {
          private DebsCellDelegate delegate;
          @Override
          public void open(Configuration parameters) throws Exception {
            delegate = new DebsCellDelegate(DebsCellDelegate.Query.FREQUENT_ROUTE);
          }
          @Override
          public void flatMap(String s, Collector<Tuple3<Long, String, String>> out)
              throws Exception {
            Tuple7<String, Long, Long, String, String, Float, Float> ride = delegate
                .deserializeRide(s);
            if (ride != null)
              out.collect(new Tuple3<>(ride.f2, ride.f3, ride.f4));
          }
        });
    compactRides
        .partitionByRange(0)
        .sortPartition(0, Order.ASCENDING)
        .writeAsCsv(args[1], "\n", ",");
    env.execute();
  }
}
