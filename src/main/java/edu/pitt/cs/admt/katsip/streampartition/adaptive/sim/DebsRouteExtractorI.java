package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim;

import com.google.common.base.Charsets;
import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.extractor.IKeyExtractor;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Created by Nikos R. Katsipoulakis on 1/22/2017.
 */
public class DebsRouteExtractorI implements IKeyExtractor<Tuple7<String, Long, Long, String, String, Float, Float>> {
    @Override
    public byte[] extractField(Tuple7<String, Long, Long, String, String, Float, Float> ride) {
        return (ride.f3 + "-" + ride.f4).getBytes(Charsets.UTF_8);
    }
}
