package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor;

import java.nio.ByteBuffer;

/**
 * Created by Nikos R. Katsipoulakis on 1/18/2017.
 */
public class IntegerExtractor implements IKeyExtractor<Integer> {
    @Override
    public byte[] extractField(Integer integer) {
        return ByteBuffer.allocate(4).putInt(integer).array();
    }
}
