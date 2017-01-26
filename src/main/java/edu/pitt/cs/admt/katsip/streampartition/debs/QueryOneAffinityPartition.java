package edu.pitt.cs.admt.katsip.streampartition.debs;

import edu.pitt.cs.admt.katsip.streampartition.flink.NaiveAffinityCardPartitioner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Nikos R. Katsipoulakis on 1/23/2017.
 */
public class QueryOneAffinityPartition {
    public static void submit(StreamExecutionEnvironment env, DataStream<Tuple3<Long, String, Integer>> timestampedRideStream, int parallelism) throws Exception {
        // Phase 1: parallel partial aggregation
        DataStream<Tuple3<Long, String, Integer>> parallelComputation = timestampedRideStream
                .partitionCustom(new NaiveAffinityCardPartitioner(), 1)
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new AllWindowFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<Long, String, Integer>> input, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                        HashMap<String, Integer> partialFrequencyIndex = new HashMap<>();
                        for (Tuple3<Long, String, Integer> t : input)
                            if (partialFrequencyIndex.containsKey(t.f1))
                                partialFrequencyIndex.put(t.f1, partialFrequencyIndex.get(t.f1) + t.f2);
                            else
                                partialFrequencyIndex.put(t.f1, t.f2);
                        for (String key : partialFrequencyIndex.keySet())
                            out.collect(new Tuple3<>(window.getEnd(), key, partialFrequencyIndex.get(key)));
                    }
                })
                .setParallelism(1);
        // Phase 3: serial full aggregation
        DataStream<String> serialAggregation = parallelComputation
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(30)))
                .apply(new AllWindowFunction<Tuple3<Long, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<Long, String, Integer>> values, Collector<String> out) throws Exception {
                        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");
                        TreeMap<Integer, List<String>> index = new TreeMap<>();
                        for (Tuple3<Long, String, Integer> event : values) {
                            if (index.containsKey(event.f2)) {
                                List<String> tmp = index.get(event.f2);
                                if (!tmp.contains(event.f1)) {
                                    tmp.add(event.f1);
                                    index.put(event.f2, tmp);
                                }
                            } else {
                                List<String> tmp = new LinkedList<>();
                                tmp.add(event.f1);
                                index.put(event.f2, tmp);
                            }
                        }
                        List<String> topTen = new LinkedList<>();
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("{");
                        for (Integer count : index.descendingKeySet()) {
                            List<String> route = index.get(count);
                            for (String r : route) {
                                topTen.add(r);
                                stringBuilder.append(r + ":" + count + ",");
                            }
                            if (topTen.size() >= 10)
                                break;
                        }
                        stringBuilder.append("}");
                        String s = "From: " + dateFormat.format(new Date(window.getStart())) + ", To: " + dateFormat.format(new Date(window.getEnd())) +
                                ", Most frequent routes: " + stringBuilder.toString();
                        out.collect(s);
                    }
                }).setParallelism(1);
        serialAggregation.print();
        env.execute();
    }
}
