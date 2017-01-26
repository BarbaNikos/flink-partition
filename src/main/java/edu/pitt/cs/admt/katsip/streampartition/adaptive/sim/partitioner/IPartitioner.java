package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.partitioner;

import java.util.AbstractMap;
import java.util.Collection;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public interface IPartitioner<T> {
    enum PARTITION_STATE {
        WINDOW_BATCH,
        COMPLETE
    }

    void init(Collection<T> source);

    PARTITION_STATE partitionNextBatch(AbstractMap<Integer, Collection<T>> buffers);

    void partition(T record, AbstractMap<Integer, Collection<T>> buffers);

    Collection<Integer> getOperatorList();
}
