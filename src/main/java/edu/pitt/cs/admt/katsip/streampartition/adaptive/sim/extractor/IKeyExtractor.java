package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public interface IKeyExtractor<TInputRecord> {
  byte[] extractField(TInputRecord record);
}
