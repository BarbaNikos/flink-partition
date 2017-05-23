package edu.pitt.cs.admt.katsip.streampartition.cardestimator;

/**
 * Created by Nikos R. Katsipoulakis on 1/6/2017.
 */
public interface ICardEstimator {
  void update(byte[] raw);

  long estimateCardinality();

  boolean contains(byte[] raw);
}
