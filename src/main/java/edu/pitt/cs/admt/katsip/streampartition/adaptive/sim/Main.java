package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim;

import com.google.common.base.Charsets;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.IKeyExtractor;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.ITimeExtractor;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.operator.Operator;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner.FieldPartitioner;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner.IPartitioner;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner.Partitioner;
import edu.pitt.cs.admt.katsip.streampartition.debs.util.DebsCellDelegate;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class Main {
  
  private static List<Tuple2<Long, String>> parseRides() throws IOException {
    List<Tuple2<Long, String>> rides = new LinkedList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader("Z:\\Documents\\flinkplayground\\src\\main\\resources\\debs_rides_first10k.csv"))) {
      DebsCellDelegate delegate = new DebsCellDelegate(DebsCellDelegate.Query.FREQUENT_ROUTE);
      for (String line; (line = reader.readLine()) != null; ) {
        Tuple7<String, Long, Long, String, String, Float, Float> ride = delegate.deserializeRide(line);
        if (ride != null) {
          Tuple2<Long, String> rideTuple = new Tuple2<>(ride.f2, ride.f3 + "-" + ride.f4);
          rides.add(rideTuple);
        }
      }
    }
    return rides;
  }
  
  public static void main(String[] args) throws IOException {
    List<Tuple2<Long, String>> rideStream = parseRides();
    List<Integer> operatorIds = new ArrayList<>();
    AbstractMap<Integer, Collection<Tuple2<Long, String>>> buffers = new HashMap<>();
    for (int i = 1; i <= 32; ++i) {
      operatorIds.add(i);
      buffers.put(i, new LinkedList<Tuple2<Long, String>>());
    }
    ITimeExtractor<Tuple2<Long, String>> timeExtractor = new RideTimeExtractor();
    IKeyExtractor<Tuple2<Long, String>> extractor = new RouteKeyExtractor();
    Partitioner<Tuple2<Long, String>> fieldPartitioner = new FieldPartitioner<>(operatorIds, Time.of(30, TimeUnit.MINUTES), extractor, timeExtractor);
    List<Operator<Tuple7<String, Long, Long, String, String, Float, Float>, Tuple2<String, Integer>>> operators = new ArrayList<>();
    fieldPartitioner.init(rideStream);
    
    while (fieldPartitioner.partitionNextBatch(buffers) != IPartitioner.PARTITION_STATE.COMPLETE) {

    }
  }
  
  public static class RideTimeExtractor implements ITimeExtractor<Tuple2<Long, String>> {
    @Override
    public Time getTime(Tuple2<Long, String> t) {
      return Time.of(t.f0, TimeUnit.MILLISECONDS);
    }
  }
  
  public static class RouteKeyExtractor implements IKeyExtractor<Tuple2<Long, String>> {
    @Override
    public byte[] extractField(Tuple2<Long, String> ride) {
      return (ride.f1).getBytes(Charsets.UTF_8);
    }
  }
}
