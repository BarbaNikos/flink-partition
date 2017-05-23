package edu.pitt.cs.admt.katsip.streampartition.util;

import com.google.common.collect.Sets;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.Histogram;

import java.util.*;

/**
 * Created by Nikos R. Katsipoulakis on 5/22/17.
 */
public class SimUtils {
  public static <K> double JaccardSimilarity(Collection<K> a, Collection<K> b) {
    Set<K> aKeys = new HashSet<>();
    Set<K> bKeys = new HashSet<>();
    for (K element : a)
      aKeys.add(element);
    for (K element : b)
      bKeys.add(element);
    return Sets.intersection(aKeys, bKeys).size() / Sets.union(aKeys, bKeys).size();
  }
  
  public static <K> double RankedBiasedDistance(List<K> a, List<K> b, double p, int k) {
    return 1.0f - RankedBiasedOverlap(a, b, p, k);
  }
  
  public static <K> double RankedBiasedOverlap(List<K> a, List<K> b, double p, int k) {
    Set<K> a_d = new HashSet<>();
    Set<K> b_d = new HashSet<>();
    int identicalElements = 0;
    double sumOfAgreements = 0.0f, d = 1.0f;
    if (a.size() == b.size() && a.size() == 0) return 1.0f;
    for (int i = 0; i < k; ++i) {
      if (b.size() <= i || a.size() <= i) break;
      if (a.get(i) == b.get(i)) identicalElements += 1;
      a_d.add(a.get(i));
      b_d.add(b.get(i));
      Set<K> intersection = Sets.intersection(a_d, b_d);
      int overlap = intersection.size();
      double agreement = ((double) overlap) / d;
      sumOfAgreements += (Math.pow(p, d - 1.0f) * agreement);
      d += 1.0f;
    }
    if (identicalElements == k)
      return 1.0f;
    else
      return (1.0f - p) * sumOfAgreements;
  }

  public static TreeMap<Integer, Integer> MergeTreeMap(TreeMap<Integer, Integer> accumulator,
                                  TreeMap<Integer, Integer> addition) {
    for (Map.Entry<Integer, Integer> entry : addition.entrySet()) {
      if (accumulator.containsKey(entry.getKey())) {
        Integer current = accumulator.get(entry.getKey());
        accumulator.put(entry.getKey(), current + entry.getValue());
      } else {
        accumulator.put(entry.getKey(), entry.getValue());
      }
    }
    return accumulator;
  }

  public static DescriptiveStatistics GatherHistogramAccumulator(
      TreeMap<Integer, Integer> accumulator, double multiplier) {
    // Produce Descriptive Statistics for each value
    DescriptiveStatistics statistics = new DescriptiveStatistics();
    for (Map.Entry<Integer, Integer> entry : accumulator.entrySet()) {
      Double key = ((double) entry.getKey()) / multiplier;
      Integer frequency = entry.getValue();
      for (int i = 0; i < frequency; ++i)
        statistics.addValue(key);
    }
    return statistics;
  }
}
