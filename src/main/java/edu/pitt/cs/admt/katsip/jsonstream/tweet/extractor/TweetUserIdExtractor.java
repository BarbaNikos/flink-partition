package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class TweetUserIdExtractor extends RichMapFunction<String, Long> {

    private LongCounter parseTime;

    private Logger log = LoggerFactory.getLogger(TweetUserIdExtractor.class);

    private long totalTime;

    public static final String accumulatorName = "tweet-user-id-parse-time";

    private JsonFactory jsonFactory;

    public void init() {
        jsonFactory = new JsonFactory();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jsonFactory = new JsonFactory();
        parseTime = new LongCounter();
        getRuntimeContext().addAccumulator(TweetUserIdExtractor.accumulatorName, this.parseTime);
    }

    @Override
    public void close() throws Exception {
        this.parseTime.add(totalTime);
        System.out.println("object with id: " + this.hashCode() + " took " + totalTime + " (msec) to parse.");
    }

    @Override
    public Long map(String value) throws Exception {
        String tName = null;
        JsonToken token = null;
        JsonParser jp = jsonFactory.createParser(value);
        boolean inUser = false;
        Long userId = -1l;
        long start = System.currentTimeMillis();
        while (userId < 0) {
            token = jp.nextToken();
            if (token == JsonToken.FIELD_NAME) {
                tName = jp.getCurrentName();
                switch (tName) {
                    case "user":
                        inUser = true;
                        break;
                    case "id":
                        if (inUser && userId < 0) {
                            jp.nextToken();
                            userId = jp.getLongValue();
                            inUser = false;
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
