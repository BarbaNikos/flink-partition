package edu.pitt.cs.admt.katsip.jsonstream.tweet.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Nikos R. Katsipoulakis on 1/19/2017.
 */
public class TweetUserIdRetweetCountExtractorTest {
    @Test
    public void TweetUserId() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TweetUserIdExtractor extractor = new TweetUserIdExtractor();
        extractor.open(new Configuration());
        TweetUserIdRetweetCountExtractor userIdRetweetCountExtractor = new TweetUserIdRetweetCountExtractor();
        userIdRetweetCountExtractor.open(new Configuration());
        TweetInReplytoScreenNameExtractor inReplytoScreenNameExtractor = new TweetInReplytoScreenNameExtractor();
        inReplytoScreenNameExtractor.open(new Configuration());
        try (BufferedReader reader = new BufferedReader(new FileReader("src" + File.separator + "main" + File.separator + "resources" + File.separator + "first_1000_tweets.json"))) {
            for (String line; (line = reader.readLine()) != null;) {
                Map<String, Object> tweetData = (Map<String, Object>) mapper.readValue(line, Map.class);
                Map<String, Object> userData = (Map<String, Object>) tweetData.get("user");
                String inReplyToScreenName = (String) tweetData.get("in_reply_to_screen_name");
                String screenName = (String) userData.get("screen_name");
                Integer retweetCount = (Integer) tweetData.get("retweet_count");
                Long userId = -1l;
                if (userData.get("id") instanceof Long) {
                    userId = (Long) userData.get("id");
                } else {
                    userId = new Long((Integer) userData.get("id"));
                }
                Long parsedId = extractor.map(line);
                Assert.assertEquals("different values for user-id", userId, parsedId);
                Assert.assertEquals("different values for user-id from user-id and retweet-count extractor",
                        userId, userIdRetweetCountExtractor.map(line).f0);
                Assert.assertEquals("different values for retweet-count from user-id and retweet-count extractor",
                        retweetCount, userIdRetweetCountExtractor.map(line).f1);
                Assert.assertEquals("different values for in-reply-to-screen-name",
                        inReplyToScreenName, inReplytoScreenNameExtractor.map(line).f0);
                Assert.assertEquals("different values for screen-name", screenName,
                        inReplytoScreenNameExtractor.map(line).f1);
            }
        }
    }

    @Test
    public void DeleteParse() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        DeleteUserIdExtractor userIdExtractor = new DeleteUserIdExtractor();
        DeleteIdStrUserIdExtractor idStrUserIdExtractor = new DeleteIdStrUserIdExtractor();
        userIdExtractor.open(new Configuration());
        idStrUserIdExtractor.open(new Configuration());
        try (BufferedReader reader = new BufferedReader(new FileReader("src" + File.separator + "main" + File.separator + "resources" + File.separator + "first_100_deletes.json"))) {
            for (String line; (line = reader.readLine()) != null;) {
                Map<String, Object> deleteData = mapper.readValue(line, Map.class);
                Map<String, Object> statusData = (Map<String,Object>) ((Map<String,Object>) deleteData.get("delete")).get("status");
                String idStr = (String) statusData.get("id_str");
                Long userId = -1l;
                if (statusData.get("user_id") instanceof Long) {
                    userId = (Long) statusData.get("user_id");
                } else {
                    userId = new Long((Integer) statusData.get("user_id"));
                }
                Assert.assertEquals("user-id extractor parsed a different value", userId, userIdExtractor.map(line));
                Assert.assertEquals("user-id + id-str extractor parsed a different value for user-id", userId, idStrUserIdExtractor.map(line).f0);
                Assert.assertEquals("user-id + id-str extractor parsed a different value for id_str", idStr, idStrUserIdExtractor.map(line).f1);
            }
        }
    }

}