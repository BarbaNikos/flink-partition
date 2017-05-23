package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.operator;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class IntegerGroupCountOperator extends Operator<Integer, Tuple2<Integer, Integer>> {

  private DescriptiveStatistics statistics;

  public IntegerGroupCountOperator() {
    super();
    statistics = new DescriptiveStatistics();
  }

  public void processWindow(Collection<Integer> inputWindow, Collection<Tuple2<Integer, Integer>> output) {
    long start = System.currentTimeMillis();
    HashMap<Integer, Integer> state = new HashMap<>();
    for (Integer record : inputWindow) {
      if (state.containsKey(record))
        state.put(record, state.get(record) + 1);
      else
        state.put(record, new Integer(1));
    }
    for (Integer record : state.keySet()) {
      output.add(new Tuple2<>(record, state.get(record)));
    }
    long end = System.currentTimeMillis();
    statistics.addValue(Math.abs(end - start));
  }

  public DescriptiveStatistics getStatistics() {
    return statistics.copy();
  }
}
