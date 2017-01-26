package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class DeleteIdStrUserIdExtractor extends RichMapFunction<String, Tuple2<Long, String>> {

    public static final String accumulatorName = "deleteIdStrUserId-parse-count";

    private Logger log = LoggerFactory.getLogger(DeleteIdStrUserIdExtractor.class);

    private LongCounter parseTime;

    private long totalTime;

    private JsonFactory jsonFactory;

    @Override
    public void open(Configuration configuration) {
        jsonFactory = new JsonFactory();
        parseTime = new LongCounter();
        getRuntimeContext().addAccumulator(accumulatorName, this.parseTime);
    }

    @Override
    public void close() {
        parseTime.add(totalTime);
        log.info("extractor with id: " + this.hashCode() + " spent " + totalTime + " (msec) on parsing.");
        System.out.println("object with id: " + this.hashCode() + " took " + totalTime + " (msec) to parse.");
    }

    @Override
    public Tuple2<Long, String> map(String value) throws Exception {
        String tName = null;
        JsonToken token = null;
        JsonParser jp = jsonFactory.createParser(value);
        boolean inStatus = false;
        Long userId = -1l;
        String idStr = "";
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
                            if (idStr.equals("") == false) {
                                long end = System.currentTimeMillis();
                                totalTime += Math.abs(end - start);
                                jp.close();
                                return new Tuple2<>(userId, idStr);
                            }
                        }
                        break;
                    case "id_str":
                        if (inStatus && idStr.equals("")) {
                            jp.nextToken();
                            idStr = jp.getValueAsString();
                            if (userId > 0) {
                                long end = System.currentTimeMillis();
                                totalTime += Math.abs(end - start);
                                jp.close();
                                return new Tuple2<>(userId, idStr);
                            }
                        }

                }
            }
        }
        long end = System.currentTimeMillis();
        totalTime += Math.abs(end - start);
        jp.close();
        return null;
    }
}
