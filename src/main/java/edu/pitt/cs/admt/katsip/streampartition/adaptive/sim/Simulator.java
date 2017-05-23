package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim;

import com.google.common.base.Preconditions;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.aggregator.Aggregator;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.operator.Operator;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner.Partitioner;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class Simulator<TWorkerInput, TAggregatorInput> {
  
  private Integer BATCH_SIZE;
  
  private Partitioner<TWorkerInput> partitioner;
  
  private List<Operator<TWorkerInput, TAggregatorInput>> worker;
  
  private Aggregator<TAggregatorInput> aggregator;
  
  private HashMap<Integer, Collection<TWorkerInput>> partitions = new HashMap<Integer, Collection<TWorkerInput>>();
  
  public Simulator(Integer window_size, Partitioner<TWorkerInput> partitioner, List<Operator<TWorkerInput, TAggregatorInput>> worker, Aggregator<TAggregatorInput> aggregator) {
    Preconditions.checkArgument(window_size > 0, "window size must be greated than zero");
    this.BATCH_SIZE = window_size;
    this.partitioner = partitioner;
    this.worker = worker;
    this.aggregator = aggregator;
    Collection<Integer> operators = partitioner.getOperatorList();
    for (Integer o : operators)
      partitions.put(o, new LinkedList<TWorkerInput>());
  }
  
  public void step(Collection<TWorkerInput> nextBatch) {
    for (Integer o : partitions.keySet())
      partitions.get(o).clear();
    Collection<TAggregatorInput> aggregationInput = new LinkedList<TAggregatorInput>();
    //partitioner.partitionNext(nextBatch, partitions);
    for (Integer i : partitions.keySet()) {
      Collection<TAggregatorInput> output = new LinkedList<TAggregatorInput>();
      worker.get(i).processWindow(partitions.get(i), output);
      aggregationInput.addAll(output);
    }
    aggregator.aggregate(aggregationInput);
  }
  
  public void simulate(Collection<TWorkerInput> stream) {
    Preconditions.checkNotNull(stream);
    Preconditions.checkArgument(stream.size() > 0, "empty stream in argument list");
    List<TWorkerInput> buffer = new LinkedList<TWorkerInput>();
    for (TWorkerInput element : stream) {
      buffer.add(element);
      if (buffer.size() >= BATCH_SIZE) {
        step(buffer);
        buffer.clear();
      }
    }
  }
}
