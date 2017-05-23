package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner;

import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.ITimeExtractor;
import org.apache.flink.api.common.time.Time;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public abstract class Partitioner<T> implements IPartitioner<T> {
  
  protected List<Integer> workers;
  
  protected Time window;
  
  private Collection<T> source;
  
  private Iterator<T> sourceIterator;
  
  private ITimeExtractor<T> timeExtractor;
  
  private Time windowEnd;
  
  private T remainderRecord;
  
  protected Partitioner(List<Integer> workers) {
    this.workers = workers;
  }
  
  protected Partitioner(List<Integer> workers, Time window, ITimeExtractor<T> timeExtractor) {
    this(workers);
    this.window = window;
    this.timeExtractor = timeExtractor;
    this.windowEnd = null;
    sourceIterator = null;
    remainderRecord = null;
  }
  
  public void init(Collection<T> source) {
    this.source = source;
    this.sourceIterator = source.iterator();
    remainderRecord = null;
    windowEnd = null;
  }
  
  public PARTITION_STATE partitionNextBatch(AbstractMap<Integer, Collection<T>> buffers) {
    while (true) {
      T next = remainderRecord;
      remainderRecord = null;
      if (next == null) {
        if (!sourceIterator.hasNext())
          return PARTITION_STATE.COMPLETE;
        next = sourceIterator.next();
      }
      if (this.windowEnd == null)
        windowEnd = Time.of(timeExtractor.getTime(next).toMilliseconds() + window.toMilliseconds(), TimeUnit.MILLISECONDS);
      if (timeExtractor.getTime(next).toMilliseconds() > windowEnd.toMilliseconds()) {
        this.windowEnd = null;
        return PARTITION_STATE.WINDOW_BATCH;
      } else {
        partition(next, buffers);
      }
    }
  }
  
  public void partition(T record, AbstractMap<Integer, Collection<T>> buffers) {
    // do-nothing
  }
  
  public Collection<Integer> getOperatorList() {
    return workers;
  }
}
