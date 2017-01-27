package edu.pitt.cs.admt.katsip.streampartition.debs.util;

import org.apache.flink.api.java.tuple.Tuple7;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Auxiliary class to deserialize a ride's information to a Tuple7 object. The deserialization process involves
 * calculating the pickup and dropoff cell for a ride, according to the specifications of the ACM DEBS 2015 Grand
 * Challenge (@href{http://www.debs2015.org/call-grand-challenge.html}). According to the query specification, the
 * cells are calculated differently.
 *
 * Created by Nikos R. Katsipoulakis on 1/17/2017.
 */
public class DebsCellDelegate {

    public enum Query {
        FREQUENT_ROUTE,
        PROFIT_CELL
    }

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss");

    private static final double latitude = 41.474937;

    private static final double longitude = -74.913585;

    private static int cellDistance = 500; // can be 250 for query 2

    private static int gridDistance = 300; // can be 600 for query 2

    private Query QUERY_TYPE = Query.FREQUENT_ROUTE;

    private Map<String, double[]> cell;

    public DebsCellDelegate(DebsCellDelegate.Query query) {
        QUERY_TYPE = query;
        switch (query) {
            case FREQUENT_ROUTE:
                cell = DebsCoordinateUtil.getCellCoordinates(latitude, longitude, cellDistance, gridDistance);
                break;
            case PROFIT_CELL:
                cellDistance = 250;
                gridDistance = 600;
                cell = DebsCoordinateUtil.getCellCoordinates(latitude, longitude, cellDistance, gridDistance);
                break;
            default:
                cell = DebsCoordinateUtil.getCellCoordinates(latitude, longitude, cellDistance, gridDistance);
                break;
        }
    }

    public Tuple7<String, Long, Long, String, String, Float, Float> deserializeRide(String ride) {
        double pickupLatitude = -1, pickupLongitude = -1, dropoffLatitude = -1, dropoffLongitude = -1;
        String pickup, dropoff, medallion;
        long pickupTimestamp = -1l, dropoffTimestamp = -1l;
        float fare_amount = -1, tip_amount = -1;
        String[] token = ride.split(",");
        try {
            pickupLatitude = Double.parseDouble(token[7]);
            pickupLongitude = Double.parseDouble(token[6]);
            dropoffLatitude = Double.parseDouble(token[9]);
            dropoffLongitude = Double.parseDouble(token[8]);
        } catch (NumberFormatException e) {}
        pickup = DebsCoordinateUtil.recursiveLocation(1, gridDistance, 1, gridDistance, cell, pickupLatitude, pickupLongitude);
        dropoff = DebsCoordinateUtil.recursiveLocation(1, gridDistance, 1, gridDistance, cell, dropoffLatitude, dropoffLongitude);
        if (pickup != null && dropoff != null) {
            try {
                pickupTimestamp = simpleDateFormat.parse(token[2]).getTime();
                dropoffTimestamp = simpleDateFormat.parse(token[3]).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
            fare_amount = Float.parseFloat(token[11]);
            tip_amount = Float.parseFloat(token[14]);
            medallion = token[0];
            return new Tuple7<>(medallion, pickupTimestamp, dropoffTimestamp, pickup, dropoff, fare_amount, tip_amount);
        }
        return null;
    }

}
