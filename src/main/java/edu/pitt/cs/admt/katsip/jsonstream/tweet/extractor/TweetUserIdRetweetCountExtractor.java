package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class TweetUserIdRetweetCountExtractor extends RichMapFunction<String, Tuple2<Long, Integer>> {

    public static final String accumulatorName = "tweet-user-id-retweet-count-parse-time";

    private LongCounter parseTime;

    private long totalTime;

    private JsonFactory jsonFactory;

    public void init() {
        jsonFactory = new JsonFactory();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jsonFactory = new JsonFactory();
        parseTime = new LongCounter();
        getRuntimeContext().addAccumulator(TweetUserIdRetweetCountExtractor.accumulatorName, this.parseTime);
    }

    @Override
    public void close() throws Exception {
        parseTime.add(totalTime);
        System.out.println("object with id: " + this.hashCode() + " took " + totalTime + " (msec) to parse.");
    }

    @Override
    public Tuple2<Long, Integer> map(String value) throws Exception {
        JsonToken token = null;
        String tName = null;
        JsonParser jp = jsonFactory.createParser(value);
        boolean inUser = false;
        int fields = 0;
        Long userId = -1l;
        Integer retweetCount = -1;
        int open = -1;
        long start = System.currentTimeMillis();
        while (fields < 2) {
            token = jp.nextToken();
            if (token == JsonToken.FIELD_NAME) {
                tName = jp.getCurrentName();
                switch (tName) {
                    case "user":
                        inUser = true;
                        break;
                    case "retweet_count":
                        if (open == 0) {
                            jp.nextToken();
                            retweetCount = jp.getIntValue();
                            fields++;
                        }
                        break;
                    case "id":
                        if (inUser && userId < 0) {
                            jp.nextToken();
                            userId = jp.getLongValue();
                            fields++;
                            inUser = false;
                        }
                        break;
                }
            } else if (token == JsonToken.START_OBJECT) {
                open++;
            } else if (token == JsonToken.END_OBJECT && open >= 0) {
                open--;
            }
        }
        long end = System.currentTimeMillis();
        this.totalTime += Math.abs(end - start);
        jp.close();
        return new Tuple2<>(userId, retweetCount);
    }
}
