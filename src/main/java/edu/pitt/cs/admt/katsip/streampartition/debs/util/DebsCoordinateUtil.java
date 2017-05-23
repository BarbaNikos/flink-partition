package edu.pitt.cs.admt.katsip.streampartition.debs.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Nikos R. Katsipoulakis on 3/1/2016.
 */
public class DebsCoordinateUtil implements Serializable {
  
  private static final double R = 6371000;
  
  /**
   * This method receives a latitude and a longitude and moves it according to the distance and the bearing
   *
   * @param latitude
   * @param longitude
   * @param distance  in meters
   * @param bearing   in degrees (clockwise from north), (north: 0, east: 90, south: 180, west: 270)
   * @return the new coordinates
   */
  public static double[] getCoordinatePoint(double latitude, double longitude, double distance, double bearing) {
    double[] coordinates = new double[2];
    double delta = distance / R;
    double radLatitude = Math.toRadians(latitude);
    double radBearing = Math.toRadians(bearing);
    
    coordinates[0] = Math.toDegrees(Math.asin(Math.sin(radLatitude) * Math.cos(delta) + Math.cos(radLatitude) * Math.sin(delta) * Math.cos(radBearing)));
    coordinates[1] = longitude + Math.toDegrees(Math.atan2(
        Math.sin(radBearing) * Math.sin(delta) * Math.cos(radLatitude),
        (Math.cos(delta) - Math.sin(radLatitude) * Math.sin(Math.toRadians(coordinates[0])))
    ));
    return coordinates;
  }
  
  /**
   * It receives the center coordinates of a cell, and returns a vector of size 8, with the coordinates
   * of the cell's vertices. In the vector, the cell's vertices are divided into pairs, starting from the
   * latitude followed by the longitude.
   *
   * @param latitude
   * @param longitude
   * @param squareSideLength the cell's side length in meters
   * @return a vector of size 8 with the coordinate's of the cell's vertices in the following sequence north-east latitude,
   * north-east longitude, south-east latitude, south-east longitude,
   * south-west longitude, south-west latitude, north-west latitude, north-west longitude.
   */
  public static double[] getSquareEdges(double latitude, double longitude, double squareSideLength) {
    double alpha = squareSideLength * Math.sqrt(2);
    double[] northWestPoint = getCoordinatePoint(latitude, longitude, alpha, 315);
    double[] northEastPoint = getCoordinatePoint(latitude, longitude, alpha, 45);
    double[] southEastPoint = getCoordinatePoint(latitude, longitude, alpha, 135);
    double[] southWestPoint = getCoordinatePoint(latitude, longitude, alpha, 225);
    double[] square = new double[8];
    square[0] = northEastPoint[0];
    square[1] = northEastPoint[1];
    square[2] = southEastPoint[0];
    square[3] = southEastPoint[1];
    square[4] = southWestPoint[0];
    square[5] = southWestPoint[1];
    square[6] = northWestPoint[0];
    square[7] = northWestPoint[1];
    return square;
  }
  
  /**
   * It produces a @see {@link java.util.Map} with the coordinates of each cell on the produced grid. Each cell is found
   * on the map based on its name identified by a @see {@link java.lang.String} "x.y".
   *
   * @param latitude  the latitude of cell 1.1
   * @param longitude the longitude of cell 1.1
   * @param distance  the distance in meters of the side of each square
   * @return a Map with the 4 edge points of each square (north-east-point, south-east-point, south-west-point, north-west-point)
   */
  public static Map<String, double[]> getCellCoordinates(double latitude, double longitude, double distance, double gridDistance) {
    double centerLatitude;
    double centerLongitude;
    Map<String, double[]> cell = new HashMap<>();
    double firstRowLatitude = latitude;
    double firstRowLongitude = longitude;
    for (int i = 1; i <= gridDistance; i++) {
      if (i > 1) {
        double[] coordinates = getCoordinatePoint(firstRowLatitude, firstRowLongitude, distance, 180);
        firstRowLatitude = coordinates[0];
        firstRowLongitude = coordinates[1];
      }
      centerLatitude = firstRowLatitude;
      centerLongitude = firstRowLongitude;
      for (int j = 1; j <= gridDistance; j++) {
        double[] coordinates;
        if (j > 1) {
          coordinates = getCoordinatePoint(centerLatitude, centerLongitude, distance, 90);
        } else {
          coordinates = new double[2];
          coordinates[0] = centerLatitude;
          coordinates[1] = centerLongitude;
        }
        double[] square = getSquareEdges(coordinates[0], coordinates[1], distance);
        cell.put(new String(i + "." + j), square);
        centerLatitude = coordinates[0];
        centerLongitude = coordinates[1];
      }
    }
    return cell;
  }
  
  /**
   * Method to calculate distance between two points by using the Haversine formula (source: http://www.movable-type.co.uk/scripts/latlong.html)
   *
   * @param latitude   of the first point
   * @param longitude  of the first point
   * @param latitude2  of the second point
   * @param longitude2 of the second point
   * @return distance in meters
   */
  public static double pointDistance(double latitude, double longitude, double latitude2, double longitude2) {
    double deltaLatitude = Math.toRadians(latitude2 - latitude);
    double deltaLongitude = Math.toRadians(longitude2 - longitude);
    double sinDeltaLatitude = Math.sin(deltaLatitude / 2);
    double sinDeltaLongitude = Math.sin(deltaLongitude / 2);
    double alpha = Math.pow(sinDeltaLatitude, 2) + Math.cos(Math.toRadians(latitude)) * Math.cos(Math.toRadians(latitude2)) * Math.pow(sinDeltaLongitude, 2);
    double c = 2 * Math.atan2(Math.sqrt(alpha), Math.sqrt(1.0 - alpha));
    return R * c;
  }
  
  /**
   * It checks whether a given coordinate is inside a cell.
   *
   * @param pointLatitude
   * @param pointLongitude
   * @param cell
   * @return true if the coordinate is inside the cell. Otherwise, it returns false
   */
  public static boolean cellMembership(double pointLatitude, double pointLongitude, double[] cell) {
    double xA = cell[0];
    double yA = cell[1];
    double xB = cell[2];
    double yB = cell[3];
    double xC = cell[4];
    double yC = cell[5];
    double xD = cell[6];
    double yD = cell[7];
    double fAB = (xA - xB) * (pointLongitude - yB) - (yA - yB) * (pointLatitude - xB);
    double fBC = (xB - xC) * (pointLongitude - yC) - (yB - yC) * (pointLatitude - xC);
    double fCD = (xC - xD) * (pointLongitude - yD) - (yC - yD) * (pointLatitude - xD);
    double fDA = (xD - xA) * (pointLongitude - yA) - (yD - yA) * (pointLatitude - xA);
    return (fAB <= 0 && fBC <= 0 && fCD <= 0 && fDA <= 0);
  }
  
  /**
   * searches recursively for the cell that a given point belongs to.
   *
   * @param minX
   * @param maxX
   * @param minY
   * @param maxY
   * @param cells
   * @param pointLatitude
   * @param pointLongitude
   * @return the cell identifier
   */
  public static String recursiveLocation(int minX, int maxX, int minY, int maxY, Map<String, double[]> cells, double pointLatitude, double pointLongitude) {
    int midX = (int) Math.floor((minX + maxX) / 2);
    int midY = (int) Math.floor((minY + maxY) / 2);
    double[] middleCell = cells.get(new String(midX + "." + midY));
    double fUpper = (middleCell[6] - middleCell[0]) * (pointLongitude - middleCell[1]) - (middleCell[7] - middleCell[1]) * (pointLatitude - middleCell[0]);
    double fRight = (middleCell[4] - middleCell[6]) * (pointLongitude - middleCell[7]) - (middleCell[5] - middleCell[7]) * (pointLatitude - middleCell[6]);
    if (fUpper > 0) {
      // point belongs in cell with x in [minX, midX)
      if (fRight > 0) {
        // point belongs in cell with y in [minY, midY)
        if (Math.abs(maxX - minX) <= 3 && Math.abs(maxY - minY) <= 3) {
          return locate(minX, midX - 1, minY, midY - 1, cells, pointLatitude, pointLongitude);
        } else {
          return recursiveLocation(minX, midX, minY, midY, cells, pointLatitude, pointLongitude);
        }
      } else {
        // point belongs in cell with y in [midY, maxY]
        if (Math.abs(maxX - minX) <= 3 && Math.abs(maxY - minY) <= 3) {
          return locate(minX, midX - 1, midY, maxY, cells, pointLatitude, pointLongitude);
        } else {
          return recursiveLocation(minX, midX, midY, maxY, cells, pointLatitude, pointLongitude);
        }
      }
    } else {
      // point belongs in cell with x in [midX, maxX]
      if (fRight > 0) {
        // point belongs in cell with y in [minY, midY)
        if (Math.abs(maxX - minX) <= 3 && Math.abs(maxY - minY) <= 3) {
          return locate(midX, maxX, minY, midY - 1, cells, pointLatitude, pointLongitude);
        } else {
          return recursiveLocation(midX, maxX, minY, midY, cells, pointLatitude, pointLongitude);
        }
      } else {
        // point belongs in cell with y in [midY, maxY]
        if (Math.abs(maxX - minX) <= 3 && Math.abs(maxY - minY) <= 3) {
          return locate(midX, maxX, midY, maxY, cells, pointLatitude, pointLongitude);
        } else {
          return recursiveLocation(midX, maxX, midY, maxY, cells, pointLatitude, pointLongitude);
        }
      }
    }
  }
  
  private static String locate(int minX, int maxX, int minY, int maxY, Map<String, double[]> cells, double pointLatitude, double pointLongitude) {
    for (int x = minX; x <= maxX; x++) {
      for (int y = minY; y <= maxY; y++) {
        String candidateCell = x + "." + y;
        if (cellMembership(pointLatitude, pointLongitude, cells.get(candidateCell)))
          return candidateCell;
      }
    }
    return null;
  }
  
}
