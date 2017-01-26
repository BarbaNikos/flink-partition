package edu.pitt.cs.admt.katsip.streampartition.flink;

import edu.pitt.cs.admt.katsip.streampartition.partitioner.NaiveCardinalityPartitioner;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;

/**
 * Created by Nikos R. Katsipoulakis on 1/6/2017.
 */
public class NaiveCardPartitioner implements Partitioner<Object> {

    private NaiveCardinalityPartitioner partitioner = null;

    @Override
    public int partition(Object key, int numPartitions) {
        byte[] raw = SerializationUtils.serialize((Serializable) key);
        if (partitioner == null) {
            partitioner = new NaiveCardinalityPartitioner();
            partitioner.init(numPartitions);
        }
        return partitioner.partitionNext(raw);
    }
}
