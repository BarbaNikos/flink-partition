package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.aggregator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/22/2017.
 */
public class DEBSFrequentRouteAggregator extends Aggregator<Tuple2<String, Integer>> {

  private DescriptiveStatistics statistics;

  public DEBSFrequentRouteAggregator() {
    statistics = new DescriptiveStatistics();
  }

  @Override
  public void aggregate(Collection<Tuple2<String, Integer>> batch) {
    long start = System.currentTimeMillis();
    HashMap<String, Integer> histogram = new HashMap<>();
    for (Tuple2<String, Integer> record : batch) {
      if (histogram.containsKey(record.f0))
        histogram.put(record.f0, histogram.get(record.f0) + record.f1);
      else
        histogram.put(record.f0, record.f1);
    }
    long end = System.currentTimeMillis();
    statistics.addValue(Math.abs(end - start));
  }

  public DescriptiveStatistics getStatistics() {
    return statistics.copy();
  }
}
