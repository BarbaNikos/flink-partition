package edu.pitt.cs.admt.katsip.streampartition.debs.frequentroute;

import edu.pitt.cs.admt.katsip.streampartition.util.SimUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.accumulators.DoubleMaximum;
import org.apache.flink.api.common.accumulators.DoubleMinimum;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nikos R. Katsipoulakis on 5/23/17.
 */
public class ShedErrorFunction extends RichJoinFunction<
    Tuple2<Long, List<Tuple2<String, Integer>>>,
    Tuple2<Long, List<Tuple2<String, Integer>>>,
    Tuple3<Long, Double, Double>> {

  public static final String NumberOfWindowsAccumulator = "SHED_ERR_WIN_NUM_ACC";

  private IntCounter numberOfWindows;

  public static final String JaccardHistogramAccumulator = "SHED_ERR_JACCARD_HIST_ACC";

  private Histogram jaccardHistogram;

  public static final String RbdHistogramAccumulator = "SHED_ERR_RBD_HIST_ACC";

  private Histogram rbdHistogram;

  private final double p;

  public ShedErrorFunction(double p) {
    this.p = p;
  }
  
  @Override
  public void open(Configuration parameters) throws Exception {
    numberOfWindows = new IntCounter();
    getRuntimeContext().addAccumulator(ShedErrorFunction.NumberOfWindowsAccumulator,
        numberOfWindows);
    jaccardHistogram = new Histogram();
    getRuntimeContext().addAccumulator(ShedErrorFunction.JaccardHistogramAccumulator,
        jaccardHistogram);
    rbdHistogram = new Histogram();
    getRuntimeContext().addAccumulator(ShedErrorFunction.RbdHistogramAccumulator, rbdHistogram);
  }

  @Override
  public void close() throws Exception {

  }
  
  @Override
  public Tuple3<Long, Double, Double> join(Tuple2<Long, List<Tuple2<String, Integer>>> first,
                                           Tuple2<Long, List<Tuple2<String, Integer>>> second)
      throws Exception {
    numberOfWindows.add(1);
    List<String> t1Keys = new ArrayList<>();
    for (Tuple2<String, Integer> t : first.f1)
      t1Keys.add(t.f0);
    List<String> t2Keys = new ArrayList<>();
    for (Tuple2<String, Integer> t : second.f1)
      t2Keys.add(t.f0);
    double rbd = SimUtils.RankedBiasedDistance(t1Keys, t2Keys, p, 10);
    rbdHistogram.add((int) rbd * 1000000);
    double jaccardSimilarity = SimUtils.JaccardSimilarity(t1Keys, t2Keys);
    jaccardHistogram.add((int) jaccardSimilarity * 1000000);
    return new Tuple3<>(first.f0, rbd, jaccardSimilarity);
  }
}
