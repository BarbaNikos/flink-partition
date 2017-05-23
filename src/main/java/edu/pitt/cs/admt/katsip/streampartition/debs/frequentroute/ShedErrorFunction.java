package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.util.SimUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 5/23/17.
 */
public class ShedErrorFunction extends RichJoinFunction<
    Tuple2<Long, List<Tuple2<String, Integer>>>,
    Tuple2<Long, List<Tuple2<String, Integer>>>,
    Tuple3<Long, Double, Double>> {
  
  private final double p;
  
  private DescriptiveStatistics jaccardStats;
  
  private DescriptiveStatistics rbdStats;
  
  public ShedErrorFunction(double p) {
    this.p = p;
  }
  
  @Override
  public void open(Configuration parameters) throws Exception {
    jaccardStats = new DescriptiveStatistics();
    rbdStats = new DescriptiveStatistics();
  }
  
  @Override
  public Tuple3<Long, Double, Double> join(Tuple2<Long, List<Tuple2<String, Integer>>> first,
                                           Tuple2<Long, List<Tuple2<String, Integer>>> second)
      throws Exception {
    List<String> t1Keys = new ArrayList<>();
    for (Tuple2<String, Integer> t : first.f1)
      t1Keys.add(t.f0);
    List<String> t2Keys = new ArrayList<>();
    for (Tuple2<String, Integer> t : second.f1)
      t2Keys.add(t.f0);
    double rbd = SimUtils.RankedBiasedDistance(t1Keys, t2Keys, p);
    double jaccardSimilarity = SimUtils.JaccardSimilarity(t1Keys, t2Keys);
    return new Tuple3<>(first.f0, rbd, jaccardSimilarity);
  }
}
