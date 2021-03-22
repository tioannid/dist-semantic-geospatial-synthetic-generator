/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package geomshape;

import java.io.Serializable;

/**
 *
 * @author tioannid
 */
public class gHexagon implements IShape, Serializable {
    //     /\
    //    /  \
    //   |    |
    //   |    |
    //   \    /
    //     \/
    //
    // https://en.wikipedia.org/wiki/Hexagon
    //
    // minimal radius or inradius: r 
    // minimal diameter: d=2*r
    // maximal radius or circumradius: R
    // hexagon side length: t=R
    // FORMULA 1:   r=cos(30)*R=[sqrt(3)/2]*R=[sqrt(3)/2]*t
    // FORMULA 2:   d=2*r=2*[sqrt(3)/2]*t=sqrt(3)*t
    // FORMULA 3:   t=d/sqrt(3)

    // ----- STATIC MEMBERS -----
    // ----- DATA MEMEBERS -----
    double x;
    double y;
    double t;

    // ----- CONSTRUCTORS -----
    public gHexagon(double x, double y, double t) {
        this.x = x;
        this.y = y;
        this.t = t;
    }

    // ----- DATA ACCESSORS -----
    double getInRadius() {  // r
        return IShape.COS30 * t;    // r = cos(30)*t = [sqrt(3)/2]*t    
    }
    // ----- METHODS -----

    @Override
    public String getWKT() {
        double half_t = t/2;
        double r = getInRadius();
        StringBuilder sb = new StringBuilder(1024);
        sb.append("POLYGON ((");
        // P1
        sb.append(x).append(" ").append(y - t).append(", ");
        // P2
        sb.append(x + r).append(" ").append(y - half_t).append(", ");
        // P3
        sb.append(x + r).append(" ").append(y + half_t).append(", ");
        // P4
        sb.append(x).append(" ").append(y + t).append(", ");
        // P5
        sb.append(x - r).append(" ").append(y + half_t).append(", ");
        // P6
        sb.append(x - r).append(" ").append(y - half_t).append(", ");
        // P1
        sb.append(x).append(" ").append(y - t);
        sb.append("))");

        return sb.toString();
    }
}
