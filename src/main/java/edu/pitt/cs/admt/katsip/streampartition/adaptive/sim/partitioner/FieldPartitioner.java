package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.IKeyExtractor;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.ITimeExtractor;
import org.apache.flink.api.common.time.Time;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class FieldPartitioner<TInput> extends Partitioner<TInput> {

  private final HashFunction h1 = Hashing.murmur3_128(13);
  private IKeyExtractor<TInput> fieldExtractor;

  public FieldPartitioner(List<Integer> workers, IKeyExtractor<TInput> fieldExtractor) {
    super(workers);
    this.fieldExtractor = fieldExtractor;
  }

  public FieldPartitioner(List<Integer> workers, Time window, IKeyExtractor<TInput> fieldExtractor,
                          ITimeExtractor<TInput> timeExtractor) {
    super(workers, window, timeExtractor);
    this.fieldExtractor = fieldExtractor;
  }

  @Override
  public void partition(TInput record, AbstractMap<Integer, Collection<TInput>> buffers) {
    byte[] raw = fieldExtractor.extractField(record);
    int first = (int) (Math.abs(h1.hashBytes(raw).asLong()) % workers.size());
    buffers.get(first).add(record);
  }
}
