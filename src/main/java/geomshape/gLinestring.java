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
public class gLinestring implements IShape, Serializable {
    // 
    //       -------
    //      /
    // -----
    //   

    // ----- STATIC MEMBERS -----
    // ----- DATA MEMEBERS -----
    double x;
    double y;
    double t;
    double e;
    boolean forward;
    boolean vertical;
    long N;
    double maxX;
    double maxY;

    // ----- CONSTRUCTORS -----
    public gLinestring(double x, double y, double t, double e,
            boolean forward, boolean vertical, long N,
            double maxX, double maxY) {
        this.x = x;
        this.y = y;
        this.t = t;
        this.e = e;
        this.forward = forward;
        this.vertical = vertical;
        this.N = N;
        this.maxX = maxX;
        this.maxY = maxY;
    }

    // ----- DATA ACCESSORS -----
    double getInRadius() {  // r
        return IShape.COS30 * t;    // r = cos(30)*t = [sqrt(3)/2]*t    
    }
    // ----- METHODS -----

    @Override
    public String getWKT() {
        StringBuffer sb = new StringBuffer(1024);
        sb.append("LINESTRING (");

        double deltaY = 3*t/2;
        double deltaX = Math.sqrt(3)*t;
        double maxy = deltaY * ((double) this.N);
        double maxx = deltaX * ((double) this.N);

        int points = 0;
        while ((vertical && y < maxy) || ((!vertical) && x < maxx)) {
            if (vertical) {
                if (forward) {
                    x += e;
                    forward = false;
                } else {
                    x -= e / 2d;
                    forward = true;
                }

            } else {
                if (forward) {
                    y += e;
                    forward = false;
                } else {
                    y -= e / 2d;
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
                y += t * 2d;
            } else {
                x += deltaX * 2d;
            }

        }

        int pos = sb.lastIndexOf(",");
        sb.replace(pos, pos + 2, ")");

        if (points < 2) {
            return "";
        }

        return sb.toString();
    }
}
