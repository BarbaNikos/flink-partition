package edu.pitt.cs.admt.katsip.streampartition.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import edu.pitt.cs.admt.katsip.streampartition.cardestimator.NaiveCardEstimator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/4/2017.
 */
public class NaiveAffinityPartitioner implements IPartitioner {
  
  private final HashFunction h1 = Hashing.murmur3_128(13);
  
  private final HashFunction h2 = Hashing.murmur3_128(17);
  
  private List<NaiveCardEstimator> cardinality;
  
  private int partitionNum;
  
  @Override
  public void init(int partitionNum) {
    this.cardinality = new ArrayList<NaiveCardEstimator>();
    for (int i = 0; i < partitionNum; ++i)
      this.cardinality.add(new NaiveCardEstimator());
    this.partitionNum = partitionNum;
  }
  
  @Override
  public int partitionNext(byte[] raw) {
    int first = (int) (Math.abs(h1.hashBytes(raw).asLong()) % partitionNum);
    int second = (int) (Math.abs(h2.hashBytes(raw).asLong()) % partitionNum);
    if (this.cardinality.get(first).contains(raw)) {
      return first;
    } else if (this.cardinality.get(second).contains(raw)) {
      return second;
    } else {
      if (this.cardinality.get(first).estimateCardinality() < this.cardinality.get(second).estimateCardinality()) {
        this.cardinality.get(first).update(raw);
        return first;
      } else {
        this.cardinality.get(second).update(raw);
        return second;
      }
    }
  }
}
