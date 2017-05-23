package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.operator;

import java.util.Collection;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public abstract class Operator<T, W> implements IOperator<T, W> {

  public Operator() {

  }

  @Override
  public void processWindow(Collection<T> inputWindow, Collection<W> output) {

  }
}
