package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor;

import org.apache.flink.api.common.time.Time;

/**
 * Created by Nikos R. Katsipoulakis on 1/24/2017.
 */
public interface ITimeExtractor<TInputRecord> {
  Time getTime(TInputRecord record);
}
