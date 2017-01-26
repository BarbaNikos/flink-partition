package edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.operator;

import edu.pitt.cs.admt.katsip.streampartition.adaptive.sim.DebsRouteExtractor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by Nikos R. Katsipoulakis on 1/22/2017.
 */
public class DEBSFrequentRouteOperator extends Operator<Tuple7<String,Long,Long,String,String,Float,Float>,Tuple2<String,Integer>> {

    private DebsRouteExtractor extractor;

    private DescriptiveStatistics statistics;

    public DEBSFrequentRouteOperator() {
        super();
        extractor = new DebsRouteExtractor();
        statistics = new DescriptiveStatistics();
    }

    public void processWindow(Collection<Tuple7<String,Long,Long,String,String,Float,Float>> inputWindow, Collection<Tuple2<String, Integer>> output) {
        long start = System.currentTimeMillis();
        HashMap<String, Integer> state = new HashMap<>();
        for (Tuple7<String,Long,Long,String,String,Float,Float> record : inputWindow) {
            String route = record.f3 + "-" + record.f4;
            if (state.containsKey(route))
                state.put(route, state.get(record) + 1);
            else
                state.put(route, new Integer(1));
        }
        for (String route : state.keySet()) {
            output.add(new Tuple2<>(route, state.get(route)));
        }
        long end = System.currentTimeMillis();
        statistics.addValue(Math.abs(end - start));
    }

    public DescriptiveStatistics getStatistics() { return statistics.copy(); }
}
