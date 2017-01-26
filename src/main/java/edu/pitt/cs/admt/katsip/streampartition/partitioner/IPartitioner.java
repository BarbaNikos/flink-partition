package edu.pitt.cs.admt.katsip.streampartition.partitioner;

import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 1/4/2017.
 */
public interface IPartitioner {
    void init(int partitionNum);
    int partitionNext(byte[] raw);
}
