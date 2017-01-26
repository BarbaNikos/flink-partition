package edu.pitt.cs.admt.katsip.streampartition.cardestimator;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Nikos R. Katsipoulakis on 1/6/2017.
 */
public class NaiveCardEstimator implements ICardEstimator {

    private Set<byte[]> estimator;

    public NaiveCardEstimator() {
        estimator = new HashSet<>();
    }

    @Override
    public void update(byte[] raw) {
        estimator.add(raw);
    }

    @Override
    public long estimateCardinality() {
        return estimator.size();
    }

    @Override
    public boolean contains(byte[] raw) {
        return estimator.contains(raw);
    }
}
