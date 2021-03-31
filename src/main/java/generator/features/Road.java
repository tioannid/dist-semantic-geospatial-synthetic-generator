/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package generator.features;

import generator.DistDataSyntheticGenerator;
import geomshape.gLinestring;
import geomshape.gPoint;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author tioannid
 */
public class Road implements Serializable {    // Small Hexagon

    // ----- STATIC MEMBERS -----
    static final String className = "Road";
    static final String prefix = "http://geographica.di.uoa.gr/generator/road/";
    static long classInstanceId = 0;

    // ----- STATIC METHODS -----
    static synchronized long getClassInstanceId() {
        return ++classInstanceId;
    }

    // ----- DATA MEMBERS -----
    long id;
    double x, y, e;
    boolean forward, vertical;
    DistDataSyntheticGenerator g;

    // ----- CONSTRUCTORS -----
    public Road(double x, double y, double e,
            boolean forward, boolean vertical, DistDataSyntheticGenerator g) {
        this.id = getClassInstanceId(); // get id and increment it
        this.x = x;
        this.y = y;
        this.e = e;
        this.forward = forward;
        this.vertical = vertical;
        this.g = g;
    }

    // ----- DATA ACCESSORS -----
    // ----- METHODS -----
    public Iterator<String> getTriples() {
        List<String> triples = new ArrayList<>();
        // some optimizations
        String prefixID = prefix + id;
        String prefixGeometryId = prefix + "geometry/" + id;
        String prefixIdTag = prefix + id + "/tag/";
        String prefixIdTagId;
        double hexSide = g.SMALL_HEX_SIDE.getValue();
        long N = g.SMALL_HEX_PER_AXIS.getValue();
        double maxX = g.MAXX.getValue();
        double maxY = g.MAXY.getValue();
        String wkt = new gLinestring(x, y, hexSide, e, forward, vertical,
                N, maxX, maxY).getWKT();
        int MAX_TAG_VALUE = g.TAG_VALUE.getValue();

        // feature is class
        triples.add("<" + prefixID + "/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + prefix + className + "> .");
        // feature has geometry
        triples.add("<" + prefixID + "/> <" + prefix + "hasGeometry> <" + prefixGeometryId + "/> .");
        // geometry has serialization
        triples.add("<" + prefixGeometryId + "/> <" + prefix + "asWKT> \"" + wkt + "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .");

        for (int tagId = 1; (id % tagId == 0) && tagId <= MAX_TAG_VALUE; tagId *= 2) {
            if (tagId > 1 && tagId < MAX_TAG_VALUE) {
                continue;
            }
            // in loop optimization
            prefixIdTagId = prefixIdTag + tagId;
            // feature has tagId
            triples.add("<" + prefixID + "/> <" + prefix + "hasTag> <" + prefixIdTagId + "/> .");
            // tagId is Tag
            triples.add("<" + prefixIdTagId + "/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + prefix + "Tag> .");
            // tagId has key
            triples.add("<" + prefixIdTagId + "/> <" + prefix + "hasKey> \"" + tagId + "\" .");
            // tagId has value
            triples.add("<" + prefixIdTagId + "/> <" + prefix + "hasValue> \"" + tagId + "\" .");
        }
        return triples.iterator();
    }
}
