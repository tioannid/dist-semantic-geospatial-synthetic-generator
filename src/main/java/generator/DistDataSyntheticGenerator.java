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
import geomshape.Shape;
import geomshape.gHexagon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.Path;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import utils.Utils;

/**
 * @author Theofilos Ioannidis <tioannid@di.uoa.gr>
 */
public class DistDataSyntheticGenerator {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("DistDataSyntheticGenerator");

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
    int smallHexagonsPerAxis;
    public Broadcast<Integer> SMALL_HEX_PER_AXIS;
    public Broadcast<Double> SMALL_HEX_SIDE;
    // State = large hexagon, data members
    public Broadcast<Double> LARGE_HEX_SIDE;
    public Broadcast<Double> MAXX, MAXY;

    // folder where the output files will be stored
    // one file per geometry type will be created
    Path output;

    // maximum tag value
    Integer MAX_TAG_VALUE;
    public Broadcast<Integer> TAG_VALUE;

    JavaRDD<Road> roadRDD;

    static SparkConf conf;
    static JavaSparkContext sc;
    static SparkSession spark;

    List<Integer> landOwnershipRowList;
    JavaRDD<Integer> landOwnershipRowRDD;
    List<Integer> stateRowList;
    JavaRDD<Integer> stateRowRDD;
    List<Integer> stateCenterRowList;
    JavaRDD<Integer> stateCenterRowRDD;
    List<Integer> poiRowList;
    JavaRDD<Integer> poiRowRDD;

    // ----- CONSTRUCTORS -----
    /**
     * @param hdfsOutputPath
     * @param smallHexagonsPerAxis The number of small hexagons that will be
     * generated along an axis. smallHexagonsPerAxis^2 hexagons will be
     * generated.
     */
    public DistDataSyntheticGenerator(String hdfsOutputPath, int smallHexagonsPerAxis) {
        this.smallHexagonsPerAxis = smallHexagonsPerAxis;
        output = new Path(hdfsOutputPath);
        this.MAX_TAG_VALUE = smallHexagonsPerAxis;
//        while (smallHexagonsPerAxis < MAX_TAG_VALUE) {
//            MAX_TAG_VALUE >>= 1; // divide by 2
//            //MAX_TAG_VALUE = MAX_TAG_VALUE / 2;
//        }
    }

    // ----- DATA ACCESSORS -----
    public int getSmallHexagonsPerAxis() {
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
    public int getLargeHexagonsPerAxis() {
        return (getSmallHexagonsPerAxis() / 3);
    }

    public double getLargeHexagonSide() {
        return (3 * getSmallHexagonSide());
    }

    public double getLargeHexagonDx() {
        return (3 * getSmallHexagonDx());
    }

    static class GetLandOwnershipTriples implements FlatMapFunction<Integer, String> {

        private final int n, maxtag;
        private final double dx, side, x, y;

        public GetLandOwnershipTriples(int n, double dx, double side, int maxtag) {
            this.n = n;
            this.dx = dx;
            this.side = side;
            this.x = dx / 2;
            this.y = side;
            this.maxtag = maxtag;
        }

        @Override
        public Iterator<String> call(Integer row) throws Exception {
            Set<String> triples = new HashSet();
            LandOwnership lndown = null;
            double dy = (3d * y / 2d);
            double rowx, rowy = y + (row.intValue() - 1) * dy;

            //generate a line
            if (row % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (dx / 2d);
            }
            for (int col = 1; col <= n; col++) {
                lndown = new LandOwnership(rowx, rowy, side, maxtag);
                triples.addAll(lndown.getTriplesList());
                rowx += dx;
            }
            return triples.iterator();
        }
    }

    static class GetStateTriples implements FlatMapFunction<Integer, String> {

        private final int n, maxtag;
        private final double dx, side, x, y;

        public GetStateTriples(int n, double dx, double side, int maxtag) {
            this.n = n;
            this.dx = dx;
            this.side = side;
            this.x = dx / 2;
            this.y = side;
            this.maxtag = maxtag;
        }

        @Override
        public Iterator<String> call(Integer row) throws Exception {
            Set<String> triples = new HashSet();
            State state = null;
            double dy = (3d * y / 2d);
            double rowx, rowy = y + (row.intValue() - 1) * dy;

            //generate a line
            if (row % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (dx / 2d);
            }
            for (int col = 1; col <= n; col++) {
                state = new State(rowx, rowy, side, maxtag);
                triples.addAll(state.getTriplesList());
                rowx += dx;
            }
            return triples.iterator();
        }
    }

    static class GetStateCenterTriples implements FlatMapFunction<Integer, String> {

        private final int n, maxtag;
        private final double dx, x, y;

        public GetStateCenterTriples(int n, double dx, double side, int maxtag) {
            this.n = n;
            this.dx = dx;
            this.x = dx / 2;
            this.y = side;
            this.maxtag = maxtag;
        }

        @Override
        public Iterator<String> call(Integer row) throws Exception {
            Set<String> triples = new HashSet();
            StateCenter stateCenter = null;
            double dy = (3d * y / 2d);
            double rowx, rowy = y + (row.intValue() - 1) * dy;

            //generate a line
            if (row % 2 == 1) {
                rowx = x;
            } else {
                rowx = x + (dx / 2d);
            }
            for (int col = 1; col <= n; col++) {
                stateCenter = new StateCenter(rowx, rowy, maxtag);
                triples.addAll(stateCenter.getTriplesList());
                rowx += dx;
            }
            return triples.iterator();
        }
    }

    static class GetPoiTriples implements FlatMapFunction<Integer, String> {

        private final int n, maxtag;
        private final double dy, maxX, minX, minY;

        public GetPoiTriples(int n, double dy, double maxX, double minX, double minY, int maxtag) {
            this.n = n;
            this.dy = dy;
            this.maxX = maxX;
            this.minX = minX;
            this.minY = minY;
            this.maxtag = maxtag;
        }

        @Override
        public Iterator<String> call(Integer row) throws Exception {
            Set<String> triples = new HashSet();
            PointOfInterest poi = null;
            double maxDx, x1, x2, y1, y2, slope, dx, x3, y3, tmp;
            maxDx = (maxX - minX) / ((double) n);
            dx = maxDx / (double) n;
            y2 = dy * ((double) n);
            x1 = minX + (double) (row.intValue() - 1) * maxDx;
            y1 = minY;
            x2 = x1 + maxDx;
            slope = (y2 - y1) / maxDx;
            for (int col = 1; col <= n; col++) {
                tmp = (double) col * dx;
                x3 = x1 + tmp;
                y3 = slope * tmp + y1;
                poi = new PointOfInterest(x3, y3, maxtag);
                triples.addAll(poi.getTriplesList());
            }
            return triples.iterator();
        }
    }

    /**
     *
     */
    public static void main(String[] args) {
        // check number of arguments
        if (args.length < 4) {
            System.err.println("Usage: SyntheticGenerator <FILEFORMAT> <OUTPUTPATH> <N> <PARTITIONS>");
            System.err.println("       where <FILEFORMAT> is the file format {text | parquet} for the output files,");
            System.err.println("       where <OUTPUT PATH> is the folder where the generated RDF files will be stored,");
            System.err.println("             <N> is number of generated land ownership (small hexagons) along the x axis");
            System.err.println("             <PARTITIONS> is number of partitions to use");
        }
        // read arguments
        String fileFormat = args[0];
        String hdfsOutputPath = args[1];
        int N = Integer.parseInt(args[2]);
        int partitions = new Integer(args[3]);

        // check if fileFormat is correct
        if (!(fileFormat.equals("text") || fileFormat.equals("parquet"))) {
            System.err.println("Usage: SyntheticGenerator <FILEFORMAT> <OUTPUTPATH> <N> <PARTITIONS>");
            System.err.println("       where <FILEFORMAT> is the file format {text | parquet} for the output files,");
            System.err.println("       where <OUTPUT PATH> is the folder where the generated RDF files will be stored,");
            System.err.println("             <N> is number of generated land ownership (small hexagons) along the x axis");
            System.err.println("             <PARTITIONS> is number of partitions to use");
        }

        // create Spark session, Java spark context and Spark conf
        spark = SparkSession.builder().appName("Distributed Synthetic Generator - " + Utils.prettyPrint(args))
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", AvgRegistrator.class.getName())
                .getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        conf = sc.getConf();

        // start time measurement for the main part of the program
        long startTime = System.nanoTime();
        DistDataSyntheticGenerator g = new DistDataSyntheticGenerator(hdfsOutputPath, N);
        g.SMALL_HEX_PER_AXIS = sc.broadcast(g.smallHexagonsPerAxis);
        g.MAXX = sc.broadcast(g.maxX);
        g.MAXY = sc.broadcast(g.maxY);
        g.TAG_VALUE = sc.broadcast(g.MAX_TAG_VALUE);
        g.SMALL_HEX_SIDE = sc.broadcast(g.getSmallHexagonSide());
        g.LARGE_HEX_SIDE = sc.broadcast(g.getLargeHexagonSide());

        // ----------------- 1) Land Ownership generation ----------------
        long landOwnershipStart = System.nanoTime();
        // create a list of land ownership rows 
        g.landOwnershipRowList = new ArrayList<>();
        for (int i = 1; i <= g.getSmallHexagonsPerAxis(); i++) {
            g.landOwnershipRowList.add(i);
        }
        // parallelize Land Ownerships
        g.landOwnershipRowRDD = (partitions != 0)
                ? sc.parallelize(g.landOwnershipRowList, partitions)
                : sc.parallelize(g.landOwnershipRowList);
        logger.info("ALERT-DEBUG 0: Number of g.landOwnershipRowRDD records after parallelizing : " + g.landOwnershipRowRDD.count());
        // produce Land Ownership triples
        JavaRDD<String> landOwnershipTriplesRDD
                = g.landOwnershipRowRDD.flatMap(
                        new GetLandOwnershipTriples(
                                g.getSmallHexagonsPerAxis(),
                                g.getSmallHexagonDx(),
                                g.getSmallHexagonSide(),
                                g.MAX_TAG_VALUE.intValue()));
        logger.info("ALERT-DEBUG 0: Number of landOwnershipTriplesRDD records : " + landOwnershipTriplesRDD.count());
        logger.info("num of partitions of landOwnershipTriplesRDD = " + landOwnershipTriplesRDD.getNumPartitions());
        // store Land Ownership triples to HDFS file
        Dataset<String> landOwnershipTriplesDF = spark.createDataset(landOwnershipTriplesRDD.rdd(), Encoders.STRING());        
//        if (fileFormat.equalsIgnoreCase("text")) {
//            landOwnershipTriplesDF.write().text(hdfsOutputPath + Shape.HEXAGON_SMALL.name());
//        } else if (fileFormat.equalsIgnoreCase("parquet")) {
//            landOwnershipTriplesDF.write().parquet(hdfsOutputPath + Shape.HEXAGON_SMALL.name());
//        }
        landOwnershipTriplesDF.write().format(fileFormat).save(hdfsOutputPath + Shape.HEXAGON_SMALL.name());
        long landOwnershipEnd = System.nanoTime();
        g.landOwnershipRowList = null;
        g.landOwnershipRowRDD = null;
        landOwnershipTriplesDF = null;
        long landOwnershipDuration = landOwnershipEnd - landOwnershipStart;
        long landOwnershipSecs = (long) (landOwnershipDuration / Math.pow(10, 9));
        long landOwnershipMins = (landOwnershipSecs / 60);
        landOwnershipSecs = landOwnershipSecs - (landOwnershipMins * 60);

        // ----------------- 2) State generation ----------------
        long stateStart = System.nanoTime();
        // create a list of State rows 
        g.stateRowList = new ArrayList<>();
        for (int i = 1; i <= (g.getLargeHexagonsPerAxis()); i++) {
            g.stateRowList.add(i);
        }
        // parallelize States
        g.stateRowRDD = (partitions != 0)
                ? sc.parallelize(g.stateRowList, partitions)
                : sc.parallelize(g.stateRowList);
        logger.info("ALERT-DEBUG 1: Number of g.stateRowRDD records after parallelizing : " + g.stateRowRDD.count());
        // produce State triples
        JavaRDD<String> stateTriplesRDD
                = g.stateRowRDD.flatMap(
                        new GetStateTriples(
                                g.getLargeHexagonsPerAxis(),
                                g.getLargeHexagonDx(),
                                g.getLargeHexagonSide(),
                                g.MAX_TAG_VALUE.intValue()));
        logger.info("ALERT-DEBUG 1: Number of stateTriplesRDD records : " + stateTriplesRDD.count());
        logger.info("num of partitions of stateTriplesRDD = " + stateTriplesRDD.getNumPartitions());
        // store State triples to HDFS file
        Dataset<String> stateTriplesDF = spark.createDataset(stateTriplesRDD.rdd(), Encoders.STRING());
//        if (fileFormat.equalsIgnoreCase("text")) {
//            stateTriplesDF.write().text(hdfsOutputPath + Shape.HEXAGON_LARGE.name());
//        } else if (fileFormat.equalsIgnoreCase("parquet")) {
//            stateTriplesDF.write().parquet(hdfsOutputPath + Shape.HEXAGON_LARGE.name());
//        }
        stateTriplesDF.write().format(fileFormat).save(hdfsOutputPath + Shape.HEXAGON_LARGE.name());
        long stateEnd = System.nanoTime();
        g.stateRowList = null;
        g.stateRowRDD = null;
        stateTriplesDF = null;
        long stateDuration = stateEnd - stateStart;
        long stateSecs = (long) (stateDuration / Math.pow(10, 9));
        long stateMins = (stateSecs / 60);
        stateSecs = stateSecs - (stateMins * 60);

        // ----------------- 3) State Center generation ----------------
        long stateCenterStart = System.nanoTime();
        // create a list of State Center rows 
        g.stateCenterRowList = new ArrayList<>();
        for (int i = 1; i <= (g.getLargeHexagonsPerAxis()); i++) {
            g.stateCenterRowList.add(i);
        }
        // parallelize State Centers
        g.stateCenterRowRDD = (partitions != 0)
                ? sc.parallelize(g.stateCenterRowList, partitions)
                : sc.parallelize(g.stateCenterRowList);
        logger.info("ALERT-DEBUG 2: Number of g.stateCenterRowRDD records after parallelizing : " + g.stateCenterRowRDD.count());
        // produce State Center triples
        JavaRDD<String> stateCenterTriplesRDD
                = g.stateCenterRowRDD.flatMap(
                        new GetStateCenterTriples(
                                g.getLargeHexagonsPerAxis(),
                                g.getLargeHexagonDx(),
                                g.getLargeHexagonSide(),
                                g.MAX_TAG_VALUE.intValue()));
        logger.info("ALERT-DEBUG 2: Number of stateCenterTriplesRDD records : " + stateCenterTriplesRDD.count());
        logger.info("num of partitions of stateCenterTriplesRDD = " + stateCenterTriplesRDD.getNumPartitions());
        // store State Center triples to HDFS file
        Dataset<String> stateCenterTriplesDF = spark.createDataset(stateCenterTriplesRDD.rdd(), Encoders.STRING());
//        if (fileFormat.equalsIgnoreCase("text")) {
//            stateCenterTriplesDF.write().text(hdfsOutputPath + Shape.HEXAGON_LARGE_CENTER.name());
//        } else if (fileFormat.equalsIgnoreCase("parquet")) {
//            stateCenterTriplesDF.write().parquet(hdfsOutputPath + Shape.HEXAGON_LARGE_CENTER.name());
//        }
        stateCenterTriplesDF.write().format(fileFormat).save(hdfsOutputPath + Shape.HEXAGON_LARGE_CENTER.name());
        long stateCenterEnd = System.nanoTime();
        g.stateCenterRowList = null;
        g.stateCenterRowRDD = null;
        stateCenterTriplesDF = null;
        long stateCenterDuration = stateCenterEnd - stateCenterStart;
        long stateCenterSecs = (long) (stateCenterDuration / Math.pow(10, 9));
        long stateCenterMins = (stateCenterSecs / 60);
        stateCenterSecs = stateCenterSecs - (stateCenterMins * 60);

        // ----------------- 4) Points of Interest generation ----------------
        long poiStart = System.nanoTime();
        // create a list of Points of Interest rows 
        g.poiRowList = new ArrayList<>();
        for (int i = 1; i <= g.getSmallHexagonsPerAxis(); i++) {
            g.poiRowList.add(i);
        }
        // parallelize Points of Interest
        g.poiRowRDD = (partitions != 0)
                ? sc.parallelize(g.poiRowList, partitions)
                : sc.parallelize(g.poiRowList);
        logger.info("ALERT-DEBUG 3: Number of g.poiRowRDD records after parallelizing : " + g.poiRowRDD.count());
        // produce Points of Interest triples
        JavaRDD<String> poiTriplesRDD
                = g.poiRowRDD.flatMap(
                        new GetPoiTriples(
                                g.getSmallHexagonsPerAxis(),
                                g.getSmallHexagonDy(),
                                g.maxX,
                                g.minX,
                                g.minY,
                                g.MAX_TAG_VALUE.intValue()));
        logger.info("ALERT-DEBUG 3: Number of poiTriplesRDD records : " + poiTriplesRDD.count());
        logger.info("num of partitions of poiTriplesRDD = " + poiTriplesRDD.getNumPartitions());
        // store Points of Interest triples to HDFS file
        Dataset<String> poiTriplesDF = spark.createDataset(poiTriplesRDD.rdd(), Encoders.STRING());
//        if (fileFormat.equalsIgnoreCase("text")) {
//            poiTriplesDF.write().text(hdfsOutputPath + Shape.POINT.name());
//        } else if (fileFormat.equalsIgnoreCase("parquet")) {
//            poiTriplesDF.write().parquet(hdfsOutputPath + Shape.POINT.name());
//        }
        poiTriplesDF.write().format(fileFormat).save(hdfsOutputPath + Shape.POINT.name());
        long poiEnd = System.nanoTime();
        g.poiRowList = null;
        g.poiRowRDD = null;
        poiTriplesDF = null;
        long poiDuration = poiEnd - poiStart;
        long poiSecs = (long) (poiDuration / Math.pow(10, 9));
        long poiMins = (poiSecs / 60);
        poiSecs = poiSecs - (poiMins * 60);

        // Roads generation
        long roadStart = System.nanoTime();
        try {
            g.roadRDD = (partitions != 0)
                    ? sc.parallelize(g.generateRoads(), partitions)
                    : sc.parallelize(g.generateRoads());
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
        logger.info("ALERT-DEBUG 4: Number of g.roadRDD records after parallelizing : " + g.roadRDD.count());
        JavaRDD<String> roadTriplesRDD = g.roadRDD.flatMap(road -> road.getTriples());
        logger.info("ALERT-DEBUG 4: Number of roadTriplesRDD records : " + roadTriplesRDD.count());
        logger.info("num of partitions of roadTriplesRDD = " + roadTriplesRDD.getNumPartitions());
        Dataset<String> roadTriplesDF = spark.createDataset(roadTriplesRDD.rdd(), Encoders.STRING());
//        if (fileFormat.equalsIgnoreCase("text")) {
//            roadTriplesDF.write().text(hdfsOutputPath + Shape.LINESTRING.name());
//        } else if (fileFormat.equalsIgnoreCase("parquet")) {
//            roadTriplesDF.write().parquet(hdfsOutputPath + Shape.LINESTRING.name());
//        }
        roadTriplesDF.write().format(fileFormat).save(hdfsOutputPath + Shape.LINESTRING.name());
        long roadEnd = System.nanoTime();
        g.roadRDD.unpersist();
        g.roadRDD = null;
        long roadDuration = roadEnd - roadStart;
        long roadSecs = (long) (roadDuration / Math.pow(10, 9));
        long roadMins = (roadSecs / 60);
        roadSecs = roadSecs - (roadMins * 60);
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
        double maxDx, x1, x2, y1, y2, slope, dx, x3, y3;
        maxDx = (maxX - minX) / ((double) this.smallHexagonsPerAxis);
        dx = maxDx / (double) this.smallHexagonsPerAxis;
        y2 = this.getSmallHexagonDy() * ((double) this.smallHexagonsPerAxis); //maxy
        for (int i = 0; i < this.smallHexagonsPerAxis; i++) {
            x1 = minX + (double) i * maxDx;
            y1 = minY;
            x2 = x1 + maxDx;
            // y2 is constant to the maximum y
            slope = (y2 - y1) / maxDx;
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

    public static class AvgRegistrator implements KryoRegistrator {

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(LandOwnership.class, new FieldSerializer(kryo, LandOwnership.class));
            kryo.register(gHexagon.class, new FieldSerializer(kryo, gHexagon.class));
        }
    }

    static public void runGC() {
        Runtime runtime = Runtime.getRuntime();
        long memoryMax = runtime.maxMemory();
        long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
        double memoryUsedPercent = (memoryUsed * 100.0) / memoryMax;
        logger.info("DBG-10 : JVM % of memory used = " + memoryUsedPercent);
        if (memoryUsedPercent > 90.0) {
            System.gc();
        }
    }
}
