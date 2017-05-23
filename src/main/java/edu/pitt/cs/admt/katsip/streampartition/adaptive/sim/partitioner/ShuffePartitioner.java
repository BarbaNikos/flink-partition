package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner;

import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.ITimeExtractor;
import org.apache.flink.api.common.time.Time;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class ShuffePartitioner<TInput> extends Partitioner<TInput> {

  private int index;

  public ShuffePartitioner(List<Integer> workers) {
    super(workers);
  }

  public ShuffePartitioner(List<Integer> workers, Time window,
                           ITimeExtractor<TInput> timeExtractor) {
    super(workers, window, timeExtractor);
  }

  @Override
  public void init(Collection<TInput> source) {
    super.init(source);
    this.index = 0;
  }

  @Override
  public void partition(TInput record, AbstractMap<Integer, Collection<TInput>> buffers) {
    buffers.get(index).add(record);
    index = index < this.workers.size() - 1 ? index++ : 0;
  }

}
