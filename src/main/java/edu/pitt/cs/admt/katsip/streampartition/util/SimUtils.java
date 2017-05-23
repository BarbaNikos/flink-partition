package edu.pitt.cs.admt.katsip.streampartition.util;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
  
  public static <K> double RankedBiasedDistance(List<K> a, List<K> b, double p) {
    return 1.0f - RankedBiasedOverlap(a, b, p);
  }
  
  public static <K> double RankedBiasedOverlap(List<K> a, List<K> b, double p) {
    Set<K> a_d = new HashSet<>();
    Set<K> b_d = new HashSet<>();
    int k = a.size(), identicalElements = 0;
    double sumOfAgreements = 0.0f, d = 1.0f;
    if (a.size() == b.size() && a.size() == 0) return 1.0f;
    for (int i = 0; i < k; ++i) {
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
}
