package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.aggregator;

import java.util.Collection;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public interface IAggregator<T> {
    void aggregate(Collection<T> batch);
}
