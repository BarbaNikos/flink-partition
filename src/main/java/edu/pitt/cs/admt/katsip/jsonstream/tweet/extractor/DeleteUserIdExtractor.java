package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class DeleteUserIdExtractor extends RichMapFunction<String, Long> {

    private LongCounter parseTime;

    private long totalTime;

    private JsonFactory jsonFactory;

    public void init() {
        jsonFactory = new JsonFactory();
    }

    @Override
    public void open(Configuration configuration) {
        jsonFactory = new JsonFactory();
        parseTime = new LongCounter();
    }

    @Override
    public void close() {
        parseTime.add(totalTime);
    }

    @Override
    public Long map(String value) throws Exception {
        String tName = null;
        JsonToken token = null;
        JsonParser jp = jsonFactory.createParser(value);
        boolean inStatus = false;
        Long userId = -1l;
        long start = System.currentTimeMillis();
        while (userId < 0) {
            token = jp.nextToken();
            if (token == JsonToken.FIELD_NAME) {
                tName = jp.getCurrentName();
                switch (tName) {
                    case "status":
                        inStatus = true;
                        break;
                    case "user_id":
                        if (inStatus && userId < 0) {
                            jp.nextToken();
                            userId = jp.getLongValue();
                            long end = System.currentTimeMillis();
                            totalTime += Math.abs(end - start);
                            jp.close();
                            return userId;
                        }
                        break;
                }
            }
        }
        long end = System.currentTimeMillis();
        totalTime += Math.abs(end - start);
        jp.close();
        return null;
    }
}
