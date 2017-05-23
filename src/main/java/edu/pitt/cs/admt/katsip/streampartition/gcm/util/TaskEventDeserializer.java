package edu.pitt.cs.admt.katsip.streampartition.gcm.util;

import org.apache.flink.api.java.tuple.Tuple13;

/**
 * Created by Nikos R. Katsipoulakis on 1/27/2017.
 */
public class TaskEventDeserializer {
  public static Tuple13<Long, Integer, Long, Long, Long, Integer, String, Integer, Integer, Float, Float, Float, Integer> deSerialize(String line) {
    String[] tokens = line.split(",");
    int iterCount = 0;
    long timestamp = -1l;
    int missingInfo = -1;
    long jobId = -1l;
    long taskIndex = -1l;
    long machineId = -1l;
    int eventType = -1;
    String userName = "";
    int schedulingClass = -1;
    int priority = -1;
    float cpuRequest = -1f, ramRequest = -1f, diskRequest = -1f;
    int constraints = -1;
    for (String t : tokens) {
      switch (iterCount) {
        case 0:
          timestamp = t.length() > 0 ? Long.parseLong(t) : 0l;
          break;
        case 1:
          missingInfo = t.length() > 0 ? Integer.parseInt(t) : 0;
          break;
        case 2:
          jobId = t.length() > 0 ? Long.parseLong(t) : 0;
          break;
        case 3:
          taskIndex = t.length() > 0 ? Long.parseLong(t) : 0;
          break;
        case 4:
          machineId = t.length() > 0 ? Long.parseLong(t) : 0;
          break;
        case 5:
          eventType = t.length() > 0 ? Integer.parseInt(t) : 0;
          break;
        case 6:
          userName = t;
          break;
        case 7:
          schedulingClass = t.length() > 0 ? Integer.parseInt(t) : 0;
          break;
        case 8:
          priority = t.length() > 0 ? Integer.parseInt(t) : 0;
          break;
        case 9:
          cpuRequest = t.length() > 0 ? Float.parseFloat(t) : 0;
          break;
        case 10:
          ramRequest = t.length() > 0 ? Float.parseFloat(t) : 0;
          break;
        case 11:
          diskRequest = t.length() > 0 ? Float.parseFloat(t) : 0;
          break;
        case 12:
          constraints = t.length() > 0 ? Integer.parseInt(t) : 0;
          break;
      }
      iterCount++;
    }
    return new Tuple13<Long, Integer, Long, Long, Long, Integer, String, Integer, Integer, Float, Float, Float, Integer>(timestamp,
        missingInfo, jobId, taskIndex, machineId, eventType, userName, schedulingClass,
        priority, cpuRequest, ramRequest, diskRequest, constraints);
  }
}
