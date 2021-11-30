/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package generator.features;

import generator.DistDataSyntheticGenerator;
import geomshape.gLinestring;
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
    double hexSide;
    long N;
    double maxX, maxY;
    int MAX_TAG_VALUE;
    boolean all_thema;

    // ----- CONSTRUCTORS -----
    public Road(double x, double y, double e,
            boolean forward, boolean vertical, DistDataSyntheticGenerator g) {
        this.id = getClassInstanceId(); // get id and increment it
        this.x = x;
        this.y = y;
        this.e = e;
        this.forward = forward;
        this.vertical = vertical;
        hexSide = g.SMALL_HEX_SIDE.getValue();
        N = g.SMALL_HEX_PER_AXIS.getValue();
        maxX = g.MAXX.getValue();
        maxY = g.MAXY.getValue();
        MAX_TAG_VALUE = g.TAG_VALUE.getValue();
        all_thema = g.isAll_thema();
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
        String wkt = new gLinestring(x, y, hexSide, e, forward, vertical,
                N, maxX, maxY).getWKT();

        if (id == 1) { // insert class level triples
            triples.add("<" + prefix + "asWKT> <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://www.opengis.net/ont/geosparql#asWKT> .");
        }

        // feature is class
        triples.add("<" + prefixID + "/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + prefix + className + "> .");
        // feature has geometry
        triples.add("<" + prefixID + "/> <" + prefix + "hasGeometry> <" + prefixGeometryId + "/> .");
        // geometry has serialization
        triples.add("<" + prefixGeometryId + "/> <" + prefix + "asWKT> \"" + wkt + "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .");

        for (int tagId = 1; (id % tagId == 0) && tagId <= MAX_TAG_VALUE; tagId *= 2) {
            if (!all_thema) { // if not ALL_THEMA needed then short circuit the intermediate tag values
                if (tagId > 1 && tagId < MAX_TAG_VALUE) {
                    continue;
                }
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
