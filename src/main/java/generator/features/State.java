/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package generator.features;

import generator.DistSyntheticGenerator;
import geomshape.gHexagon;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author tioannid
 */
public class State implements Serializable {    // Small Hexagon

    // ----- STATIC MEMBERS -----
    static final String className = "State";
    static final String prefix = "http://geographica.di.uoa.gr/generator/state/";
    static long classInstanceId = 0;

    // ----- STATIC METHODS -----
    static synchronized long getClassInstanceId() {
        return ++classInstanceId;
    }
    
    // ----- DATA MEMBERS -----
    //gHexagon hex;
    long id;
    double x, y;
    DistSyntheticGenerator g;

    // ----- CONSTRUCTORS -----
    public State(double x, double y, DistSyntheticGenerator g) {
        this.id = getClassInstanceId(); // get id and increment it
        this.x = x;
        this.y = y;
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
        double hexSide = g.LARGE_HEX_SIDE.getValue();
        String wkt = new gHexagon(x, y, hexSide).getWKT();
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
