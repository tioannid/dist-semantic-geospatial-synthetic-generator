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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @author Theofilos Ioannidis <tioannid@di.uoa.gr>
 */
public class DistDataSyntheticGenerator {

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
    HashMap<Shape, String> namedGraphs = new HashMap<DistDataSyntheticGenerator.Shape, String>() {
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
    public DistDataSyntheticGenerator(String hdfsOutputPath, long smallHexagonsPerAxis) {
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
     * @return
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
            x += ((2d - 3d / (double) (this.smallHexagonsPerAxis / 2)) * this.getSmallHexagonDx());
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
        DistDataSyntheticGenerator g = new DistDataSyntheticGenerator(hdfsOutputPath, N);
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
