/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (C) 2013, Pyravlos Team
 *
 */
package generator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import generator.features.LandOwnership;
import geomshape.Shape;
import geomshape.gHexagon;
import java.io.IOException;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import utils.Utils;

/**
 * @author Theofilos Ioannidis <tioannid@di.uoa.gr>
 */
public class DistQuerySyntheticGenerator {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("DistQuerySyntheticGenerator");

    String unit = "<http://www.opengis.net/def/uom/OGC/1.0/metre>";
//	final String unit = "<http://www.opengis.net/def/uom/OGC/1.0/degree>";

    // our Universe!
    double minX = 0;
    double maxX = 90d;
    // Our universe is rectangular :)
    double minY = minX;
    double maxY = maxX;

    // deltas of the x and y axes
    // Denote the distance of the center of two concecutive hexagons
    // when moving along the x or y axis (not both);
    // https://en.wikipedia.org/wiki/Hexagon
    // minimal radius or inradius: r 
    // minimal diameter: d=2*r
    // maximal radius or circumradius: R
    // hexagon side length: t=R
    // FORMULA 1:   r=cos(30)*R=[sqrt(3)/2]*R=[sqrt(3)/2]*t
    // FORMULA 2:   d=2*r=2*[sqrt(3)/2]*t=sqrt(3)*t
    // FORMULA 3:   t=d/sqrt(3)
    // Landownership = small hexagon, data members
    // Number of hexagons(land ownerships) per axis :)
    long smallHexagonsPerAxis;

    HashMap<Shape, String> namedGraphs = new HashMap<Shape, String>() {
        {
            put(Shape.HEXAGON_SMALL, "http://geographica.di.uoa.gr/generator/landOwnership");
            put(Shape.HEXAGON_LARGE, "http://geographica.di.uoa.gr/generator/state");
            put(Shape.LINESTRING, "http://geographica.di.uoa.gr/generator/road");
            put(Shape.POINT, "http://geographica.di.uoa.gr/generator/pointOfInterest");
            put(Shape.HEXAGON_LARGE_CENTER, "http://geographica.di.uoa.gr/generator/stateCenter");
        }
    };

    HashMap<Shape, String> namedFeatures = new HashMap<Shape, String>() {
        {
            put(Shape.HEXAGON_SMALL, "Landownerships");
            put(Shape.HEXAGON_LARGE, "States");
            put(Shape.LINESTRING, "Roads");
            put(Shape.POINT, "Pois");
            put(Shape.HEXAGON_LARGE_CENTER, "StateCenters");
        }
    };

    // the extension functions that will be used for generating queries
    enum TopologicalFunction {
        INTERSECTS, TOUCHES, WITHIN
    };
    HashMap<TopologicalFunction, String> extensionFunctions = new HashMap<TopologicalFunction, String>() {
        {
            put(TopologicalFunction.INTERSECTS, "geof:sfIntersects");
            put(TopologicalFunction.TOUCHES, "geof:sfTouches");
            put(TopologicalFunction.WITHIN, "geof:sfWithin");
        }
    };

    // prefixes
    String prefixes = " PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n"
            + " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
            + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n"
            + " PREFIX strdf: <http://strdf.di.uoa.gr/ontology#> \n"
            + " PREFIX geof: <http://www.opengis.net/def/function/geosparql/> \n"
            + " PREFIX geo: <http://www.opengis.net/ont/geosparql#> \n"
            + " PREFIX geo-sf: <http://www.opengis.net/ont/sf#> \n";

    // folder where the output files will be stored
    // one file per geometry type will be created
    Path output;

    // maximum tag value
    Integer MAX_TAG_VALUE = 8192;

    //private double[] selectivities = new double[]{0.4, 0.3, 0.2, 0.1,  0.001};
    double[] spatialSelectivities;
    static SparkConf conf;
    static JavaSparkContext sc;
    int[] thematicTagSelectivities;

    // ----- CONSTRUCTORS -----
    /**
     * @param hdfsOutputPath
     * @param smallHexagonsPerAxis The number of small hexagons that will be
     * generated along an axis. smallHexagonsPerAxis^2 hexagons will be
     * generated.
     */
    public DistQuerySyntheticGenerator(String hdfsOutputPath, int smallHexagonsPerAxis,
            List<String> spatialSelectiviesList, String thematicTagSelectiviesArg) {
        this.smallHexagonsPerAxis = smallHexagonsPerAxis;
        output = new Path(hdfsOutputPath);
        // convert spatial selectivitity string list to array of doubles
        this.spatialSelectivities = new double[spatialSelectiviesList.size()];
        for (int i = 0; i < this.spatialSelectivities.length; i++) {
            this.spatialSelectivities[i] = Double.parseDouble(spatialSelectiviesList.get(i));
            logger.info("Spatial selectivity value [:" + i + "] = " + this.spatialSelectivities[i]);
        }
        // convert thematic tag selectivities argument into an array of int tag values
        String tags[] = thematicTagSelectiviesArg.split(",");
        this.thematicTagSelectivities = new int[tags.length];
        for (int i = 0; i < tags.length; i++) {
            this.thematicTagSelectivities[i] = Integer.parseInt(tags[i]);
            logger.info("Thematic selectivity tag value [:" + tags[i] + "] = " + this.thematicTagSelectivities[i]);
        }

        this.MAX_TAG_VALUE = smallHexagonsPerAxis;
//        while (smallHexagonsPerAxis < MAX_TAG_VALUE) {
//            MAX_TAG_VALUE >>= 1; // divide by 2
//            //MAX_TAG_VALUE = MAX_TAG_VALUE / 2;
//        }
    }

    // ----- DATA ACCESSORS -----
    public long getSmallHexagonsPerAxis() {
        return smallHexagonsPerAxis;
    }

    public double getSmallHexagonDx() {
        return (maxX - minX) / (smallHexagonsPerAxis + 0.5);
    }

    public double getSmallHexagonSide() {
        return (getSmallHexagonDx() / Math.sqrt(3d));
    }

    public double getSmallHexagonDy() {
        return (3d * getSmallHexagonSide() / 2d);
    }

    public Integer getMAX_TAG_VALUE() {
        return MAX_TAG_VALUE;
    }

    // ----- METHODS -----
    public long getLargeHexagonsPerAxis() {
        return (getSmallHexagonsPerAxis() / 3);
    }

    public double getLargeHexagonSide() {
        return (3 * getSmallHexagonSide());
    }

    public double getLargeHexagonDx() {
        return (3 * getSmallHexagonDx());
    }

    String getQueryName(int function, int queryType, String featuresPart) {
        // function name
        String functionName = "";
        switch (function) {
            case 0:
                functionName = "Intersects";
                break;
            case 1:
                functionName = "Touches";
                break;
            case 2:
                functionName = "Within";
                break;
        }
        // query type
        String type = "";
        switch (queryType) {
            case 0:
                type = "Selection";
                break;
            case 1:
                type = "Join";
                break;
        }
        String queryName = "Synthetic_"
                + type + "_"
                + functionName + "_"
                + featuresPart;

        return queryName;
    }

    public int getNumberOfThematicTagSelectivities() {
        return thematicTagSelectivities.length;
    }

    public String[][][][] generateQueries() {
        /* first index denotes spatial function (intersect, touch, within) : 3
           second index denotes query type (selection, join) : 2
           third index denotes the query
         */
        String[][][][] queries = new String[3][2][][];

        // Intersects
        queries[0][0] = generateSpatialSelection(TopologicalFunction.INTERSECTS, Shape.HEXAGON_SMALL);
        queries[0][1] = generateSpatialJoin(TopologicalFunction.INTERSECTS, Shape.HEXAGON_SMALL, Shape.HEXAGON_LARGE);

        // Touches
        // skip selections
        queries[1][0] = new String[2][queries[0][0][0].length];
        for (int i = 0; i < queries[0][0][0].length; i++) {
            queries[1][0][0][i] = "";
            queries[1][0][1][i] = "";
        }
        queries[1][1] = generateSpatialJoin(TopologicalFunction.TOUCHES, Shape.HEXAGON_LARGE, Shape.HEXAGON_LARGE);

        // Within
        queries[2][0] = generateSpatialSelection(TopologicalFunction.WITHIN, Shape.POINT);
        queries[2][1] = generateSpatialJoin(TopologicalFunction.WITHIN, Shape.POINT, Shape.HEXAGON_LARGE);

        return queries;
    }

    public String[][][] generatePointQueries() {
//		String[][][] queries = new String[3][2][]; // TODO
        String[][][] queries = new String[1][2][]; // TODO

        // Intersects
        queries[0][0] = generateSpatialSelectionPoints(Shape.POINT);
        queries[0][1] = generateSpatialJoinPoints(Shape.POINT, Shape.HEXAGON_LARGE_CENTER);

//		// Touches
//		// skip selections
//		queries[1][0] = new String[queries[0][0].length];
//		for (int i = 0 ; i < queries[0][0].length; i++)
//			queries[1][0][i] = "";
//		queries[1][1] = generateSpatialJoin(TopologicalFunction.TOUCHES, Shape.HEXAGON_LARGE, Shape.HEXAGON_LARGE);
//
//		// Within
//		queries[2][0] = generateSpatialSelection(TopologicalFunction.WITHIN, Shape.POINT);
//		queries[2][1] = generateSpatialJoin(TopologicalFunction.WITHIN, Shape.POINT, Shape.HEXAGON_LARGE);
        return queries;
    }

    /**
     * @param shp1: The first shape to be used
     * @param shp2: The second shape to be used
     * @return
     */
    private String[] generateSpatialJoinPoints(Shape shp1, Shape shp2) {
        String[] queries = new String[4];
        String header = prefixes
                + " SELECT ?s1 ?s2 \n"
                + " WHERE {\n";
        String partA
                = //" GRAPH <" + namedGraphs.get(shp1) + "> { \n" +
                "       ?s1 <" + namedGraphs.get(shp1) + "/hasGeometry> ?s1Geo . \n"
                + "       ?s1Geo <" + namedGraphs.get(shp1) + "/asWKT> ?geo1 . \n"
                + "       ?s1 <" + namedGraphs.get(shp1) + "/hasTag> ?tag1 . \n"
                + "       ?tag1 <" + namedGraphs.get(shp1) + "/hasKey> \"KEY1\" .  \n";
        //"       }\n" +
        String partB
                = //" GRAPH <" + namedGraphs.get(shp2) + "> { \n" +
                "       ?s2 <" + namedGraphs.get(shp2) + "/hasGeometry> ?s2Geo . \n"
                + "       ?s2Geo <" + namedGraphs.get(shp2) + "/asWKT> ?geo2 . \n"
                + "       ?s2 <" + namedGraphs.get(shp2) + "/hasTag> ?tag2 . \n"
                + "       ?tag2 <" + namedGraphs.get(shp2) + "/hasKey> \"KEY2\" .  \n";
        //"       }\n" +
        String footer = "       FILTER ( " + "geof:distance" + "(?geo1, ?geo2, <http://www.opengis.net/def/uom/OGC/1.0/metre>) <= DISTANCE) .\n"
                + " }\n";

        double radius = (3d * this.getSmallHexagonSide()) * 5;
        double midX = (maxX - minX) / 2;
        double midY = (maxY - minY) / 2;
        double distanceInMeters = -1;
        try {
            WKTReader wktReader = new WKTReader();

            Geometry start = wktReader.read("POINT( " + midX + " " + midY + ")");
            Geometry end = wktReader.read("POINT( " + (midX + radius) + " " + midY + ")");

            distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:4326"));
//			distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:3857"));

            System.out.println("Start = " + start.toText() + " End = " + end.toText());
            System.out.println("Distance = " + distanceInMeters + "m ");
        } catch (ParseException | TransformException | FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        queries[0] = header + partA.replace("KEY1", "1") + partB.replace("KEY2", "1") + footer.replace("DISTANCE", Double.toString(distanceInMeters));
        queries[1] = header + partA.replace("KEY1", "1") + partB.replace("KEY2", this.MAX_TAG_VALUE.toString()) + footer.replace("DISTANCE", Double.toString(distanceInMeters));
        queries[2] = header + partB.replace("KEY2", "1") + partA.replace("KEY1", this.MAX_TAG_VALUE.toString()) + footer.replace("DISTANCE", Double.toString(distanceInMeters));
        queries[3] = header + partA.replace("KEY1", this.MAX_TAG_VALUE.toString()) + partB.replace("KEY2", this.MAX_TAG_VALUE.toString()) + footer.replace("DISTANCE", Double.toString(distanceInMeters));

        return queries;
    }

    /**
     * @param function: The topological function that will appear at the filter
     * clause
     * @param shp1: The first shape to be used
     * @param shp2: The second shape to be used
     * @return
     */
    private String[] generateSpatialSelectionPoints(Shape shp1) {
        String[] queries = new String[this.spatialSelectivities.length * 2];

        String query = prefixes
                + " SELECT ?s1 \n"
                + " WHERE {\n"
                + //" GRAPH <" + namedGraphs.get(shp1) + "> { \n" +
                "       ?s1 <" + namedGraphs.get(shp1) + "/hasGeometry> ?s1Geo . \n"
                + "       ?s1Geo <" + namedGraphs.get(shp1) + "/asWKT> ?geo1 . \n"
                + "       ?s1 <" + namedGraphs.get(shp1) + "/hasTag> ?tag1 . \n"
                + "       ?tag1 <" + namedGraphs.get(shp1) + "/hasKey> \"KEY1\" .  \n"
                + //"       }\n" +						
                //						"       FILTER ( " + extensionFunctions.get(function) + "(?geo1, \"CONSTANT\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)) .\n" +
                "       FILTER ( " + "bif:st_within" + "(?geo1, bif:st_point(45, 45), DISTANCE)) .\n"
                + " }\n";

        for (int i = 0; i < this.spatialSelectivities.length; i++) {
            String[] distanceAndCenter = defineDistanceForSelectivity(shp1, this.spatialSelectivities[i]);

            String distance = null;

            if (unit.equals("<http://www.opengis.net/def/uom/OGC/1.0/degree>")) {
                distance = distanceAndCenter[0]; // degree
            } else {
                distance = distanceAndCenter[1];  // metre
                distance = String.format("%f", (Double.parseDouble(distance) / 1000));
            }

            queries[2 * i] = query.replace("DISTANCE", distance).replace("KEY1", "1");
            queries[2 * i + 1] = query.replace("DISTANCE", distance).replace("KEY1", this.MAX_TAG_VALUE.toString());
        }

        return queries;
    }

    /**
     * @param function: The topological function that will appear at the filter
     * clause
     * @param shp1: The first shape to be used
     * @param shp2: The second shape to be used
     * @return
     */
    private String[][] generateSpatialJoin(TopologicalFunction function, Shape shp1, Shape shp2) {
        String[][] queries = new String[2][this.thematicTagSelectivities.length * this.thematicTagSelectivities.length * 2];
        String header = prefixes
                + " SELECT ?s1 ?s2 \n"
                + " WHERE {\n";
        String partA
                = //" GRAPH <" + namedGraphs.get(shp1) + "> { \n" +
                "       ?s1 <" + namedGraphs.get(shp1) + "/hasGeometry> ?s1Geo . \n"
                + "       ?s1Geo <" + namedGraphs.get(shp1) + "/asWKT> ?geo1 . \n"
                + "       ?s1 <" + namedGraphs.get(shp1) + "/hasTag> ?tag1 . \n"
                + "       ?tag1 <" + namedGraphs.get(shp1) + "/hasKey> \"KEY1\" .  \n";
        //"       }\n" +
        String partB
                = //" GRAPH <" + namedGraphs.get(shp2) + "> { \n" +
                "       ?s2 <" + namedGraphs.get(shp2) + "/hasGeometry> ?s2Geo . \n"
                + "       ?s2Geo <" + namedGraphs.get(shp2) + "/asWKT> ?geo2 . \n"
                + "       ?s2 <" + namedGraphs.get(shp2) + "/hasTag> ?tag2 . \n"
                + "       ?tag2 <" + namedGraphs.get(shp2) + "/hasKey> \"KEY2\" .  \n";
        //"       }\n" +
        String footer = "       FILTER ( " + extensionFunctions.get(function) + "(?geo1, ?geo2)) .\n"
                + " }\n";
        String queryTemplate1 = header + partA + partB + footer;
        String queryTemplate2 = header + partB + partA + footer;
        String query;
        int queryNo = 0;
        for (int i = 0; i < this.thematicTagSelectivities.length; i++) {
            for (int j = 0; j < this.thematicTagSelectivities.length; j++) {
                query = queryTemplate1;
                // in the first template use tag[i] for KEY1 and iterate tag[j] for KEY2
                queries[0][queryNo] = query.replace("KEY1", String.valueOf(this.thematicTagSelectivities[i])).replace("KEY2", String.valueOf(this.thematicTagSelectivities[j]));
                queries[1][queryNo] = namedFeatures.get(shp1) + "_" + namedFeatures.get(shp2) + "_" + String.valueOf(this.thematicTagSelectivities[i]) + "_" + String.valueOf(this.thematicTagSelectivities[j]);
                queryNo++;
            }
            for (int j = 0; j < this.thematicTagSelectivities.length; j++) {
                query = queryTemplate2;
                // in the second template use tag[i] for KEY2 and iterate tag[j] for KEY1
                queries[0][queryNo] = query.replace("KEY2", String.valueOf(this.thematicTagSelectivities[i])).replace("KEY1", String.valueOf(this.thematicTagSelectivities[j]));
                queries[1][queryNo] = namedFeatures.get(shp2) + "_" + namedFeatures.get(shp1) + "_" + String.valueOf(this.thematicTagSelectivities[i]) + "_" + String.valueOf(this.thematicTagSelectivities[j]);
                queryNo++;
            }
        }

        return queries;
    }

    /**
     * @param function: The topological function that will appear at the filter
     * clause
     * @param shp1: The first shape to be used
     * @param shp2: The second shape to be used
     * @return
     */
    private String[][] generateSpatialSelection(TopologicalFunction function, Shape shp1) {
        String[][] queries = new String[2][this.spatialSelectivities.length * this.thematicTagSelectivities.length];
        String queryTemplate = prefixes
                + " SELECT ?s1 \n"
                + " WHERE {\n"
                + //" GRAPH <" + namedGraphs.get(shp1) + "> { \n" +
                "       ?s1 <" + namedGraphs.get(shp1) + "/hasGeometry> ?s1Geo . \n"
                + "       ?s1Geo <" + namedGraphs.get(shp1) + "/asWKT> ?geo1 . \n"
                + "       ?s1 <" + namedGraphs.get(shp1) + "/hasTag> ?tag1 . \n"
                + "       ?tag1 <" + namedGraphs.get(shp1) + "/hasKey> \"KEY1\" .  \n"
                + //"       }\n" +						
                "       FILTER ( " + extensionFunctions.get(function) + "(?geo1, \"CONSTANT\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)) .\n"
                + " }\n";
        String query;
        int queryNo = 0;
        for (int i = 0; i < this.spatialSelectivities.length; i++) {
            String bb = definePolygonForSelectivity(shp1, this.spatialSelectivities[i]);
            // replace the polygon for spatial selection
            query = queryTemplate.replace("CONSTANT", bb);
            for (int j = 0; j < this.thematicTagSelectivities.length; j++) {
                // replace the tag key value
                queries[0][queryNo] = query.replace("KEY1", String.valueOf(this.thematicTagSelectivities[j]));
                queries[1][queryNo] = namedFeatures.get(shp1) + "_" + String.valueOf(this.thematicTagSelectivities[j]) + "_" + this.spatialSelectivities[i];
                queryNo++;
            }
        }

        return queries;
    }

    /**
     * @param shp: A shape (that corresponds to a specific distribution along
     * the universe
     * @param selectivity: The percentage (0-100.0) of spatial objects of type
     * shp that should be selected.
     * @return ret[0] distance in degree, ret[1] distance in metre
     */
    private String[] defineDistanceForSelectivity(Shape shp, double selectivity) {
//		StringBuffer sb = new StringBuffer(1024);
        double x1, x2, x3, x4, y1, y2, y3, y4, epsilon;
        String ret[] = new String[2];

        switch (shp) {

            case POINT:

                double lanes = Math.floor(selectivity * this.smallHexagonsPerAxis);

                // calculate the coordinates of 4 points (counter clockwise starting from the bottom left corner)
                x1 = minX;
                y1 = minY;
                x2 = lanes * (maxX - minX) / ((double) this.smallHexagonsPerAxis - 1)
                        + (selectivity - lanes) * (maxX - minX) / (Math.pow((double) this.smallHexagonsPerAxis - 1, 2));
                y2 = minY;
                x3 = x2;
                y3 = maxY;
                x4 = minX;
                y4 = maxY;

                // if possible, expand the rectangle by a small epsilon
                epsilon = (this.getSmallHexagonDx() + this.getSmallHexagonDy()) / 10d;
                if (x1 - epsilon > minX) {
                    x1 -= epsilon;
                }
                if (y1 - epsilon > minY) {
                    y1 -= epsilon;
                }
                if (x2 + epsilon < maxX) {
                    x2 += epsilon;
                }
                if (y2 - epsilon > minY) {
                    y2 -= epsilon;
                }
                if (x3 + epsilon < maxX) {
                    x3 += epsilon;
                }
                if (y3 + epsilon < maxY) {
                    y3 += epsilon;
                }
                if (x4 - epsilon > minX) {
                    x4 -= epsilon;
                }
                if (y4 + epsilon < maxY) {
                    y4 += epsilon;
                }

//			sb.append("POLYGON ((");
//			sb.append(x1);sb.append(" ");sb.append(y1);sb.append(", ");
//			sb.append(x2);sb.append(" ");sb.append(y2);sb.append(", ");
//			sb.append(x3);sb.append(" ");sb.append(y3);sb.append(", ");
//			sb.append(x4);sb.append(" ");sb.append(y4);sb.append(", ");
//			sb.append(x1);sb.append(" ");sb.append(y1);
//			sb.append("))");
//			System.out.println("Rectangle: "+sb);
                double area = (x2 - x1) * (y3 - y2);
                System.out.println("Area: " + area);
                double radius = Math.sqrt(area / Math.PI);
                ret[0] = null;
                if (radius > 45) {
                    System.out.println("Real Radius(degrees): " + radius);
                    radius = 45;
                    ret[0] = "64";
                }
                System.out.println("Radius(degrees): " + radius);

                // done!
                // Compute distance between (midX, midY) and (x3, y3)
                try {
                    WKTReader wktReader = new WKTReader();

                    double midX = 45;
                    double midY = 45;

                    Geometry start = wktReader.read("POINT( " + midX + " " + midY + ")");
                    // Radius east
                    Geometry end = wktReader.read("POINT( " + (midX + radius) + " " + midY + ")");
                    double distanceInMeterEast = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:4326"));
//				distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:3857"));
                    System.out.println("Radius east(meters): " + distanceInMeterEast);
                    // Radius west
                    end = wktReader.read("POINT( " + (midX - radius) + " " + midY + ")");
                    double distanceInMeterWest = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:4326"));
//				distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:3857"));
                    System.out.println("Radius west(meters): " + distanceInMeterWest);
                    // Radius north
                    end = wktReader.read("POINT( " + midX + " " + (midY + radius) + ")");
                    double distanceInMeterNorth = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:4326"));
//				distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:3857"));
                    System.out.println("Radius north(meters): " + distanceInMeterNorth);
                    // Radius south
                    end = wktReader.read("POINT( " + midX + " " + (midY - radius) + ")");
                    double distanceInMeterSouth = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:4326"));
//				distanceInMeters = JTS.orthodromicDistance(start.getCoordinate(), end.getCoordinate(), CRS.decode("EPSG:3857"));
                    System.out.println("Radius south(meters): " + distanceInMeterSouth);

                    double distanceInMeter = (distanceInMeterEast + distanceInMeterWest + distanceInMeterNorth + distanceInMeterSouth) / 4;
                    System.out.println("Radius mean(meters): " + distanceInMeter);

                    if (ret[0] == null) {
                        ret[0] = String.format("%f", radius);
                        if (selectivity > 0.001) // Small correction to achieve better selectiviy
                        {
                            ret[1] = String.format("%f", distanceInMeter * 0.8);
                        } else {
                            ret[1] = String.format("%f", distanceInMeter * 1.42);
                        }
                    } else {
                        ret[1] = "5000000";
                    }

                    return ret;
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (NoSuchAuthorityCodeException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (TransformException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (FactoryException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                break;

            default:
                break;
        }

        return null;
    }

    /**
     * @param shp: A shape (that corresponds to a specific distribution along
     * the universe
     * @param selectivity: The percentage (0-100.0) of spatial objects of type
     * shp that should be selected.
     * @return
     */
    private String definePolygonForSelectivity(Shape shp, double selectivity) {
        StringBuffer sb = new StringBuffer(1024);
        double x1, x2, x3, x4, y1, y2, y3, y4, epsilon;

        switch (shp) {
            case HEXAGON_SMALL:
                long nodesPerAxis = (long) Math.ceil(Math.sqrt((double) this.smallHexagonsPerAxis * (double) this.smallHexagonsPerAxis * selectivity));

                // calculate the coordinates of 4 points (counter clockwise starting from the bottom left corner)
                x1 = minX + this.getSmallHexagonDx() / 2d;
                y1 = minY + this.getSmallHexagonSide();
                x2 = x1 + (nodesPerAxis - 1) * this.getSmallHexagonDx();
                y2 = y1;
                x3 = x1 + (nodesPerAxis - 1) * this.getSmallHexagonDx();
                y3 = y1 + (nodesPerAxis - 1) * this.getSmallHexagonDy();

                x4 = x1;
                y4 = y1 + (nodesPerAxis - 1) * this.getSmallHexagonDy();

                // expand the polygon to cover the hexagons
                x1 -= this.getSmallHexagonDx() / 2d;
                y1 -= this.getSmallHexagonSide();
                x2 += this.getSmallHexagonDx();
                y2 -= this.getSmallHexagonSide();
                x3 += this.getSmallHexagonDx();
                y3 += this.getSmallHexagonSide();
                x4 -= this.getSmallHexagonDx() / 2d;
                y4 += this.getSmallHexagonSide();

                // if possible, expand the rectangle by a small epsilon
                epsilon = (this.getSmallHexagonDx() + this.getSmallHexagonDy()) / 10d;
                if (x1 - epsilon > minX) {
                    x1 -= epsilon;
                }
                if (y1 - epsilon > minY) {
                    y1 -= epsilon;
                }
                if (x2 + epsilon < maxX) {
                    x2 += epsilon;
                }
                if (y2 - epsilon > minY) {
                    y2 -= epsilon;
                }
                if (x3 + epsilon < maxX) {
                    x3 += epsilon;
                }
                if (y3 + epsilon < maxY) {
                    y3 += epsilon;
                }
                if (x4 - epsilon > minX) {
                    x4 -= epsilon;
                }
                if (y4 + epsilon < maxY) {
                    y4 += epsilon;
                }

                // done!
                sb.append("POLYGON ((");
                sb.append(x1);
                sb.append(" ");
                sb.append(y1);
                sb.append(", ");
                sb.append(x2);
                sb.append(" ");
                sb.append(y2);
                sb.append(", ");
                sb.append(x3);
                sb.append(" ");
                sb.append(y3);
                sb.append(", ");
                sb.append(x4);
                sb.append(" ");
                sb.append(y4);
                sb.append(", ");
                sb.append(x1);
                sb.append(" ");
                sb.append(y1);
                sb.append("))");
                break;

            case POINT:
                double lanes = Math.floor(selectivity * this.smallHexagonsPerAxis);

                // calculate the coordinates of 4 points (counter clockwise starting from the bottom left corner)
                x1 = minX;
                y1 = minY;
                x2 = lanes * (maxX - minX) / ((double) this.smallHexagonsPerAxis - 1)
                        + (selectivity - lanes) * (maxX - minX) / (Math.pow((double) this.smallHexagonsPerAxis - 1, 2));
                y2 = minY;
                x3 = x2;
                y3 = maxY;
                x4 = minX;
                y4 = maxY;

                // if possible, expand the rectangle by a small epsilon
                epsilon = (this.getSmallHexagonDx() + this.getSmallHexagonDy()) / 10d;
                if (x1 - epsilon > minX) {
                    x1 -= epsilon;
                }
                if (y1 - epsilon > minY) {
                    y1 -= epsilon;
                }
                if (x2 + epsilon < maxX) {
                    x2 += epsilon;
                }
                if (y2 - epsilon > minY) {
                    y2 -= epsilon;
                }
                if (x3 + epsilon < maxX) {
                    x3 += epsilon;
                }
                if (y3 + epsilon < maxY) {
                    y3 += epsilon;
                }
                if (x4 - epsilon > minX) {
                    x4 -= epsilon;
                }
                if (y4 + epsilon < maxY) {
                    y4 += epsilon;
                }

                // done!
                sb.append("POLYGON ((");
                sb.append(x1);
                sb.append(" ");
                sb.append(y1);
                sb.append(", ");
                sb.append(x2);
                sb.append(" ");
                sb.append(y2);
                sb.append(", ");
                sb.append(x3);
                sb.append(" ");
                sb.append(y3);
                sb.append(", ");
                sb.append(x4);
                sb.append(" ");
                sb.append(y4);
                sb.append(", ");
                sb.append(x1);
                sb.append(" ");
                sb.append(y1);
                sb.append("))");
                break;

            default:
                break;
        }

        return sb.toString();
    }

    public Integer returnMaxTagValue() {
        return this.MAX_TAG_VALUE;
    }

    public double[] returnSelectivities() {
        return this.spatialSelectivities;
    }

    public static class AvgRegistrator implements KryoRegistrator {

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(LandOwnership.class, new FieldSerializer(kryo, LandOwnership.class));
            kryo.register(gHexagon.class, new FieldSerializer(kryo, gHexagon.class));
        }
    }

    /**
     *
     */
    public static void main(String[] args) {
        // check number of arguments
        if (args.length < 3) {
            logger.error("Usage: SyntheticGenerator <OUTPUTPATH> <N> <SPATIAL_SELECTIVITIES> <THEMATIC_SELECTIVITIES>");
            logger.error("       where <OUTPUT PATH> is the folder where the generated query files will be stored,");
            logger.error("             <N> is number of generated land ownership (small hexagons) along the x axis");
            logger.error("             <SPATIAL_SELECTIVITIES> is the spatial selectivity list (comma separated within double-quotes)");
            logger.error("             <THEMATIC_SELECTIVITIES> is the thematic tag list (comma separated within double-quotes of 2^i values <= N)");
        }
        // read arguments
        String hdfsOutputPath = args[0];
        int N = new Integer(args[1]);
        // have to remove the initial and trailing double quotes
        String spatialSelectiviesArg = args[2].replace("\"", "");
        logger.info("Number of arguments : " + args.length);
        logger.info("Spatial selectivities argument : " + spatialSelectiviesArg);

        // read spatial selectivities list
        List<String> spatialSelectiviesList = Arrays.asList(spatialSelectiviesArg.split(","));
        for (int i = 0; i < spatialSelectiviesList.size(); i++) {
            logger.info("Spatial selectivity [" + i + "] : " + spatialSelectiviesList.get(i));
        }
        logger.info("DecimalFormatSymbols.getDecimalSeparator() = " + DecimalFormatSymbols.getInstance().getDecimalSeparator());
        // read thematic tag list
        String thematicTagSelectiviesArg = args[3].replace("\"", "");

        // create Spark conf and context
        conf = new SparkConf()
                .setAppName("Distributed Synthetic Generator - " + Utils.prettyPrint(args));
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", AvgRegistrator.class.getName());
        sc = new JavaSparkContext(conf);

        DistQuerySyntheticGenerator g = new DistQuerySyntheticGenerator(hdfsOutputPath, N, spatialSelectiviesList, thematicTagSelectiviesArg);

        // create the HDFS output path if not present
        Configuration hdfsConf = new Configuration();
        FileSystem fs = null;
        FSDataOutputStream out = null;
        Path outPath = new Path(hdfsOutputPath);
        try {
            fs = FileSystem.get(hdfsConf);
            if (fs.isDirectory(outPath)) {
                out = fs.create(new Path(hdfsOutputPath), false);
            } else {
                throw new IOException("Target folder is not a folder but a file!");
            }
        } catch (FileAlreadyExistsException ex) {
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }

        logger.info("\n\nGeneral Queries\n");
        String[][][][] q = g.generateQueries();
        int queryCnt = 0;
        Path queryFilePath = null;
        FSDataOutputStream queryFile = null;
        for (int function = 0; function < q.length; function++) { // intersect, touch, within
            String[][][] queriesForFunction = q[function];
            for (int queryType = 0; queryType < queriesForFunction.length; queryType++) { // selection, join
                if ((function == 1) && (queryType == 0)) {
                    continue;
                }
                String[][] queries = queriesForFunction[queryType];
                for (int k = 0; k < queries[0].length; k++) {
                    try {
                        logger.info(queries[0][k]);
                        queryFilePath = new Path(hdfsOutputPath + String.format("Q%02d_%s", queryCnt++, g.getQueryName(function, queryType, queries[1][k]) + ".qry"));
                        try {
                            queryFile = fs.create(queryFilePath);
                        } catch (IOException ex) {
                            logger.error(ex.getMessage());
                        }
                        byte[] bytes = queries[0][k].getBytes();
                        queryFile.write(bytes, 0, bytes.length);
                        queryFile.close();
                    } catch (IOException ex) {
                        logger.error(ex.getMessage());
                    }
                }
            }
        }
    }
}
