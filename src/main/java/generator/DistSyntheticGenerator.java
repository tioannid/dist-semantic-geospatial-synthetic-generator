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
import generator.features.PointOfInterest;
import generator.features.Road;
import generator.features.State;
import generator.features.StateCenter;
import geomshape.gHexagon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @author Theofilos Ioannidis <tioannid@di.uoa.gr>
 */
public class DistSyntheticGenerator {

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

    public Broadcast<Long> SMALL_HEX_PER_AXIS;

    public Broadcast<Double> SMALL_HEX_SIDE;

    // State = large hexagon, data members
    public Broadcast<Double> LARGE_HEX_SIDE;

    public Broadcast<Double> MAXX, MAXY;

    // all supported types
    enum Shape {
        HEXAGON_SMALL, HEXAGON_LARGE, LINESTRING, POINT, HEXAGON_LARGE_CENTER
    };
    HashMap<Shape, String> namedGraphs = new HashMap<DistSyntheticGenerator.Shape, String>() {
        {
            put(Shape.HEXAGON_SMALL, "http://geographica.di.uoa.gr/generator/landOwnership");
            put(Shape.HEXAGON_LARGE, "http://geographica.di.uoa.gr/generator/state");
            put(Shape.LINESTRING, "http://geographica.di.uoa.gr/generator/road");
            put(Shape.POINT, "http://geographica.di.uoa.gr/generator/pointOfInterest");
            put(Shape.HEXAGON_LARGE_CENTER, "http://geographica.di.uoa.gr/generator/stateCenter");
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
    public Broadcast<Integer> TAG_VALUE;

    //private double[] selectivities = new double[]{0.4, 0.3, 0.2, 0.1,  0.001};
    double[] selectivities = new double[]{1, 0.75, 0.5, 0.25, 0.1, 0.001};

    JavaRDD<LandOwnership> landOwnershipRDD;
    JavaRDD<State> stateRDD;
    JavaRDD<StateCenter> stateCenterRDD;
    JavaRDD<PointOfInterest> poiRDD;
    JavaRDD<Road> roadRDD;

    static SparkConf conf;
    static JavaSparkContext sc;

    // ----- CONSTRUCTORS -----
    /**
     * @param hdfsOutputPath
     * @param smallHexagonsPerAxis The number of small hexagons that will be
     * generated along an axis. smallHexagonsPerAxis^2 hexagons will be
     * generated.
     */
    public DistSyntheticGenerator(String hdfsOutputPath, long smallHexagonsPerAxis) {
        this.smallHexagonsPerAxis = smallHexagonsPerAxis;
        output = new Path(hdfsOutputPath);
        while (smallHexagonsPerAxis < MAX_TAG_VALUE) {
            MAX_TAG_VALUE >>= 1; // divide by 2
            //MAX_TAG_VALUE = MAX_TAG_VALUE / 2;
        }
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

    /**
     * Generates the small hexagons corresponding to land ownerships
     *
     * @throws IOException
     */
    public List<LandOwnership> generateLandOwnerships() throws IOException {
        List<LandOwnership> landOwnershipList = new ArrayList<>();
        double x, y;
        x = this.getSmallHexagonDx() / 2d;
        y = this.getSmallHexagonSide();
        double dy = (3d * y / 2d);
        double rowx, rowy = y;
        for (int i = 1; i <= this.getSmallHexagonsPerAxis(); i++) {
            //generate a line
            if (i % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (this.getSmallHexagonDx() / 2d);
            }
            for (int j = 1; j <= this.getSmallHexagonsPerAxis(); j++) {
                landOwnershipList.add(new LandOwnership(rowx, rowy, this));
                rowx += this.getSmallHexagonDx();
            }
            rowy = rowy + dy;
        }
        return landOwnershipList;
    }

    /**
     * Generates the large hexagons corresponding to states
     *
     * @throws IOException
     */
    public List<State> generateStates() throws IOException {
        List<State> stateList = new ArrayList<>();
        double x, y;
        x = this.getLargeHexagonDx() / 2d;
        y = this.getLargeHexagonSide();
        double dy = (3d * y / 2d);
        double rowx, rowy = y;
        for (int i = 1; i <= (this.getLargeHexagonsPerAxis()); i++) {
            //generate a line
            if (i % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (this.getLargeHexagonDx() / 2d);
            }
            for (int j = 1; j <= (this.getLargeHexagonsPerAxis()); j++) {
                stateList.add(new State(rowx, rowy, this));
                rowx += this.getLargeHexagonDx();
            }
            rowy = rowy + dy;
        }
        return stateList;
    }

    /**
     * Generates the large hexagon centers corresponding to state centers
     *
     * @throws IOException
     */
    public List<StateCenter> generateStateCenters() throws IOException {
        List<StateCenter> stateCenterList = new ArrayList<>();
        double x, y;
        x = this.getLargeHexagonDx() / 2d;
        y = this.getLargeHexagonSide();
        double dy = (3d * y / 2d);
        double rowx, rowy = y;
        for (int i = 1; i <= (this.getLargeHexagonsPerAxis()); i++) {
            //generate a line
            if (i % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (this.getLargeHexagonDx() / 2d);
            }
            for (int j = 1; j <= (this.getLargeHexagonsPerAxis()); j++) {
                stateCenterList.add(new StateCenter(rowx, rowy, this));
                rowx += this.getLargeHexagonDx();
            }
            rowy = rowy + dy;
        }
        return stateCenterList;
    }

    /**
     * Generates the points corresponding to points of interest
     *
     * @throws IOException
     */
    public List<PointOfInterest> generatePOIs() throws IOException {
        List<PointOfInterest> poiList = new ArrayList<>();
        double maxDx = (maxX - minX) / ((double) this.smallHexagonsPerAxis);
        double x1, x2, y1, y2, slope, dx, x3, y3;
        y2 = this.getSmallHexagonDy() * ((double) this.smallHexagonsPerAxis); //maxy
        for (int i = 0; i < this.smallHexagonsPerAxis; i++) {
            x1 = minX + (double) i * maxDx;
            y1 = minY;
            x2 = x1 + maxDx;
            // y2 is constant to the maximum y
            slope = (y2 - y1) / maxDx;
            dx = maxDx / (double) this.smallHexagonsPerAxis;
            for (int j = 1; j <= this.smallHexagonsPerAxis; j++) {
                double tmp = (double) j * dx;
                x3 = x1 + tmp;
                y3 = slope * tmp + y1;
                poiList.add(new PointOfInterest(x3, y3, this));
            }
        }
        return poiList;
    }

    /**
     * Generates the linestrings corresponding to roads
     *
     * @throws IOException
     */
    public List<Road> generateRoads() throws IOException {
        List<Road> roadList = new ArrayList<>();

        // generate(Shape.LINESTRING, this.numberOfPolygonsPerAxis / 2, this.hexagonSide, this.deltaX / 2d);
        double x, y, epsilon;
        long n = this.smallHexagonsPerAxis / 2;
        double a = this.getSmallHexagonSide();
        double dx = this.getSmallHexagonDx() / 2d;
        // Vertical lines
        epsilon = dx / 6;
        //x = 19 * this.getSmallHexagonDx() / 12; // 3d * dx + epsilon;
        x = 3d * dx + epsilon;
        y = this.getSmallHexagonSide() - epsilon / 2; // a - epsilon / 2d;
        for (int i = 1; i <= n; i++) {
            // generateInstance(shp, x, y, a, epsilon, (i % 2 == 0), true);
            // String wkt = generateLineString(x, y, a, epsilon, (i % 2 == 0), true);
            roadList.add(new Road(x, y, epsilon, (i % 2 == 0), true, this));
            //x = x + (2 - 6 / this.smallHexagonsPerAxis) * this.getSmallHexagonDx();
            x += ((2d - 3d / (double) (this.smallHexagonsPerAxis/2)) * this.getSmallHexagonDx());
        }
        // Horizontal lines
        x = this.getSmallHexagonSide() - epsilon / 2; // a - epsilon / 2d;
        y = 3d * dx + epsilon;
        for (int i = 1; i <= n; i++) {
            // generateInstance(shp, x, y, a, epsilon, (i % 2 == 0), true);
            // String wkt = generateLineString(x, y, a, epsilon, (i % 2 == 0), true);
            roadList.add(new Road(x, y, epsilon, (i % 2 == 0), false, this));
            y += (3d - 3d / (double) n) * a;
        }
        return roadList;
    }

    /**
     * @param x: x-coordinate of the hexagon's center
     * @param y: y-coordinate of the hexagon's center
     * @param t: the length of the hexagon's side
     */
    private String generateHexagon(double x, double y, double t) {

        double dx = Math.sqrt(3d) * t / 2d;

        StringBuffer sb = new StringBuffer(1024);
        sb.append("POLYGON ((");

        // P1
        sb.append(x);
        sb.append(" ");
        sb.append(y - t);
        sb.append(", ");
        // P2
        sb.append(x + dx);
        sb.append(" ");
        sb.append(y - t / 2d);
        sb.append(", ");
        // P3
        sb.append(x + dx);
        sb.append(" ");
        sb.append(y + t / 2d);
        sb.append(", ");
        // P4
        sb.append(x);
        sb.append(" ");
        sb.append(y + t);
        sb.append(", ");
        // P5
        sb.append(x - dx);
        sb.append(" ");
        sb.append(y + t / 2d);
        sb.append(", ");
        // P6
        sb.append(x - dx);
        sb.append(" ");
        sb.append(y - t / 2d);
        sb.append(", ");
        // P1
        sb.append(x);
        sb.append(" ");
        sb.append(y - t);
        sb.append("))");

        return sb.toString();
    }

    /**
     * @param x: x-coordinate of the lowest point of the line string
     * @param y: y-coordinate of the lowest point of the line string
     * @param a: the length of the hexagon's side
     * @param epsilon: a small value that will be added/substracted to the
     * x-coordinate of the linestring's points
     * @param forward: should the second point have a larger x-coordinate than
     * the first?
     * @return
     */
    private String generateLineString(double x, double y, double a, double epsilon, boolean forward, boolean vertical) {
        StringBuffer sb = new StringBuffer(1024);
        sb.append("LINESTRING (");

        double maxy = this.getSmallHexagonDy() * ((double) this.smallHexagonsPerAxis);
        double maxx = this.getSmallHexagonDx() * ((double) this.smallHexagonsPerAxis);

        int points = 0;
        while ((vertical && y < maxy) || ((!vertical) && x < maxx)) {
            if (vertical) {
                if (forward) {
                    x += epsilon;
                    forward = false;
                } else {
                    x -= epsilon / 2d;
                    forward = true;
                }

            } else {
                if (forward) {
                    y += epsilon;
                    forward = false;
                } else {
                    y -= epsilon / 2d;
                    forward = true;
                }
            }

            if (x > maxX || y > maxY) {
                break;
            }

            points++;
            sb.append(x);
            sb.append(" ");
            sb.append(y);
            sb.append(", ");

            if (vertical) {
                y += a * 2d;
            } else {
                x += (this.getSmallHexagonDx() * 2d);
            }

        }

        int pos = sb.lastIndexOf(",");
        sb.replace(pos, pos + 2, ")");

        if (points < 2) {
            return "";
        }

        return sb.toString();
    }

    /**
     * @param x: x-coordinate of the hexagon's center
     * @param y: y-coordinate of the hexagon's center
     */
    private String generatePoint(double x, double y) {
        return "POINT ( " + x + " " + y + ")";
    }

    public String[][][] generateQueries() {
        String[][][] queries = new String[3][2][];

        // Intersects
        queries[0][0] = generateSpatialSelection(TopologicalFunction.INTERSECTS, Shape.HEXAGON_SMALL);
        queries[0][1] = generateSpatialJoin(TopologicalFunction.INTERSECTS, Shape.HEXAGON_SMALL, Shape.HEXAGON_LARGE);

        // Touches
        // skip selections
        queries[1][0] = new String[queries[0][0].length];
        for (int i = 0; i < queries[0][0].length; i++) {
            queries[1][0][i] = "";
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
        String[] queries = new String[this.selectivities.length * 2];

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

        for (int i = 0; i < this.selectivities.length; i++) {
            String[] distanceAndCenter = defineDistanceForSelectivity(shp1, this.selectivities[i]);

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
    private String[] generateSpatialJoin(TopologicalFunction function, Shape shp1, Shape shp2) {
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
        String footer = "       FILTER ( " + extensionFunctions.get(function) + "(?geo1, ?geo2)) .\n"
                + " }\n";

        queries[0] = header + partA.replace("KEY1", "1") + partB.replace("KEY2", "1") + footer;
        queries[1] = header + partA.replace("KEY1", "1") + partB.replace("KEY2", this.MAX_TAG_VALUE.toString()) + footer;
        queries[2] = header + partB.replace("KEY2", "1") + partA.replace("KEY1", this.MAX_TAG_VALUE.toString()) + footer;
        queries[3] = header + partA.replace("KEY1", this.MAX_TAG_VALUE.toString()) + partB.replace("KEY2", this.MAX_TAG_VALUE.toString()) + footer;

        return queries;
    }

    /**
     * @param function: The topological function that will appear at the filter
     * clause
     * @param shp1: The first shape to be used
     * @param shp2: The second shape to be used
     * @return
     */
    private String[] generateSpatialSelection(TopologicalFunction function, Shape shp1) {
        String[] queries = new String[this.selectivities.length * 2];
        String query = prefixes
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

        for (int i = 0; i < this.selectivities.length; i++) {
            String bb = definePolygonForSelectivity(shp1, this.selectivities[i]);
            queries[2 * i] = query.replace("CONSTANT", bb).replace("KEY1", "1");
            queries[2 * i + 1] = query.replace("CONSTANT", bb).replace("KEY1", this.MAX_TAG_VALUE.toString());
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
        return this.selectivities;
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
            System.err.println("Usage: SyntheticGenerator <OUTPUTPATH> <N> <PARTITIONS>");
            System.err.println("       where <OUTPUT PATH> is the folder where the generated RDF files will be stored,");
            System.err.println("             <N> is number of generated land ownership (small hexagons) along the x axis");
            System.err.println("             <PARTITIONS> is number of partitions to use");
        }
        // read arguments
        String hdfsOutputPath = args[0];
        int N = new Integer(args[1]);
        int partitions = new Integer(args[2]);

        // create Spark conf and context
        conf = new SparkConf()
                .setAppName("Distributed Synthetic Generator - " + N);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", AvgRegistrator.class.getName());
        sc = new JavaSparkContext(conf);

        // start time measurement for the main part of the program
        long startTime = System.nanoTime();
        DistSyntheticGenerator g = new DistSyntheticGenerator(hdfsOutputPath, N);
        g.SMALL_HEX_PER_AXIS = sc.broadcast(g.smallHexagonsPerAxis);
        g.MAXX = sc.broadcast(g.maxX);
        g.MAXY = sc.broadcast(g.maxY);
        g.TAG_VALUE = sc.broadcast(g.MAX_TAG_VALUE);
        g.SMALL_HEX_SIDE = sc.broadcast(g.getSmallHexagonSide());
        g.LARGE_HEX_SIDE = sc.broadcast(g.getLargeHexagonSide());
        long landOwnershipPartitions = 0;
        long statePartitions = 0;
        long stateCenterPartitions = 0;
        long poiPartitions = 0;
        long roadPartitions = 0;

        // Land Ownership generation
        long landOwnershipStart = System.nanoTime();
        try {
            System.out.println("-------------------------------------");
            System.out.println("Generating " + Math.pow(g.getSmallHexagonsPerAxis(), 2) + " land ownerships (small hexagons)...");
            if (partitions != 0) {
                g.landOwnershipRDD = sc.parallelize(g.generateLandOwnerships(), partitions);
            } else {
                g.landOwnershipRDD = sc.parallelize(g.generateLandOwnerships());
            }
            landOwnershipPartitions = g.landOwnershipRDD.getNumPartitions();
            System.out.print("\n");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        JavaRDD<String> landOwnershipTriplesRDD = g.landOwnershipRDD.flatMap(lndown -> lndown.getTriples());
        System.out.println("num of partitions of smallHexTriplesRDD = " + landOwnershipTriplesRDD.getNumPartitions());
        landOwnershipTriplesRDD.saveAsTextFile(hdfsOutputPath + Shape.HEXAGON_SMALL.name());
        long landOwnershipEnd = System.nanoTime();
        long landOwnershipDuration = landOwnershipEnd - landOwnershipStart;
        long landOwnershipSecs = (long) (landOwnershipDuration / Math.pow(10, 9));
        long landOwnershipMins = (landOwnershipSecs / 60);
        landOwnershipSecs = landOwnershipSecs - (landOwnershipMins * 60);

        // State generation
        long stateStart = System.nanoTime();
        try {
            System.out.println("Generating " + Math.pow(g.getLargeHexagonsPerAxis(), 2) + " states (large hexagons)...");
            if (partitions != 0) {
                g.stateRDD = sc.parallelize(g.generateStates(), partitions);
            } else {
                g.stateRDD = sc.parallelize(g.generateStates());
            }
            statePartitions = g.stateRDD.getNumPartitions();
            System.out.print("\n");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        JavaRDD<String> stateTriplesRDD = g.stateRDD.flatMap(state -> state.getTriples());
        System.out.println("num of partitions of largeHexTriplesRDD = " + stateTriplesRDD.getNumPartitions());
        stateTriplesRDD.saveAsTextFile(hdfsOutputPath + Shape.HEXAGON_LARGE.name());
        long stateEnd = System.nanoTime();
        long stateDuration = stateEnd - stateStart;
        long stateSecs = (long) (stateDuration / Math.pow(10, 9));
        long stateMins = (stateSecs / 60);
        stateSecs = stateSecs - (stateMins * 60);

        // State Center generation
        long stateCenterStart = System.nanoTime();
        try {
            System.out.println("-------------------------------------");
            System.out.println("Generating " + Math.pow(g.getLargeHexagonsPerAxis(), 2) + " state center (points)...");
            g.stateCenterRDD = (partitions != 0)
                    ? sc.parallelize(g.generateStateCenters(), partitions)
                    : sc.parallelize(g.generateStateCenters());
            stateCenterPartitions = g.stateCenterRDD.getNumPartitions();
            System.out.print("\n");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        JavaRDD<String> stateCenterTriplesRDD = g.stateCenterRDD.flatMap(stateCenter -> stateCenter.getTriples());
        System.out.println("num of partitions of stateCenterTriplesRDD = " + stateCenterTriplesRDD.getNumPartitions());
        stateCenterTriplesRDD.saveAsTextFile(hdfsOutputPath + Shape.HEXAGON_LARGE_CENTER.name());
        long stateCenterEnd = System.nanoTime();
        long stateCenterDuration = stateCenterEnd - stateCenterStart;
        long stateCenterSecs = (long) (stateCenterDuration / Math.pow(10, 9));
        long stateCenterMins = (stateCenterSecs / 60);
        stateCenterSecs = stateCenterSecs - (stateCenterMins * 60);

        // Points of Interest generation
        long poiStart = System.nanoTime();
        try {
            System.out.println("-------------------------------------");
            System.out.println("Generating " + Math.pow(g.getSmallHexagonsPerAxis(), 2) + " points of interest (points)...");
            g.poiRDD = (partitions != 0)
                    ? sc.parallelize(g.generatePOIs(), partitions)
                    : sc.parallelize(g.generatePOIs());
            poiPartitions = g.poiRDD.getNumPartitions();
            System.out.print("\n");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        JavaRDD<String> poiTriplesRDD = g.poiRDD.flatMap(poi -> poi.getTriples());
        System.out.println("num of partitions of poiTriplesRDD = " + poiTriplesRDD.getNumPartitions());
        poiTriplesRDD.saveAsTextFile(hdfsOutputPath + Shape.POINT.name());
        long poiEnd = System.nanoTime();
        long poiDuration = poiEnd - poiStart;
        long poiSecs = (long) (poiDuration / Math.pow(10, 9));
        long poiMins = (poiSecs / 60);
        poiSecs = poiSecs - (poiMins * 60);

        // Roads generation
        long roadStart = System.nanoTime();
        try {
            System.out.println("-------------------------------------");
            System.out.println("Generating " + g.getSmallHexagonsPerAxis() + " roads (linestrings)...");
            g.roadRDD = (partitions != 0)
                    ? sc.parallelize(g.generateRoads(), partitions)
                    : sc.parallelize(g.generateRoads());
            roadPartitions = g.roadRDD.getNumPartitions();
            System.out.print("\n");
            System.out.println("-------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
        JavaRDD<String> roadTriplesRDD = g.roadRDD.flatMap(road -> road.getTriples());
        System.out.println("num of partitions of roadTriplesRDD = " + roadTriplesRDD.getNumPartitions());
        roadTriplesRDD.saveAsTextFile(hdfsOutputPath + Shape.LINESTRING.name());
        long roadEnd = System.nanoTime();
        long roadDuration = roadEnd - roadStart;
        long roadSecs = (long) (roadDuration / Math.pow(10, 9));
        long roadMins = (roadSecs / 60);
        roadSecs = roadSecs - (roadMins * 60);
        System.out.println("Maximum tag generated: " + g.MAX_TAG_VALUE);
        System.out.println("Execution time : " + landOwnershipMins + "min " + landOwnershipSecs + "sec");
        System.out.println("num of partitions of g.landOwnershipRDD = " + landOwnershipPartitions);
        System.out.println("Execution time : " + stateMins + "min " + stateSecs + "sec");
        System.out.println("num of partitions of g.stateRDD = " + statePartitions);
        System.out.println("Execution time : " + stateCenterMins + "min " + stateCenterSecs + "sec");
        System.out.println("num of partitions of g.stateRDD = " + stateCenterPartitions);
        System.out.println("Execution time : " + poiMins + "min " + poiSecs + "sec");
        System.out.println("num of partitions of g.stateRDD = " + poiPartitions);
        System.out.println("Execution time : " + roadMins + "min " + roadSecs + "sec");
        System.out.println("num of partitions of g.roadRDD = " + roadPartitions);
    }
}
