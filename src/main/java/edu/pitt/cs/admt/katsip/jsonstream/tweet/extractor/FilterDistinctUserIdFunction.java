package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import java.util.HashSet;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class FilterDistinctUserIdFunction extends RichFilterFunction<Long> {

    private HashSet<Long> ids;

    @Override
    public void open(Configuration parameters) throws Exception  {
        ids = new HashSet<>();
    }

    @Override
    public void close() throws Exception {
        ids.clear();
    }

    @Override
    public boolean filter(Long value) throws Exception {
        if (!ids.contains(value)) {
            ids.add(value);
            return true;
        } else {
            return false;
        }
    }
}
