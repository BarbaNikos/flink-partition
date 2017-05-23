package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.aggregator;

import java.util.Collection;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public abstract class Aggregator<T> implements IAggregator<T> {
  
  @Override
  public void aggregate(Collection<T> batch) {

  }
}
