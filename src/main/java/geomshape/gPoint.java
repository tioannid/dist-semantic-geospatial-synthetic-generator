/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package geomshape;

/**
 *
 * @author tioannid
 */
public class gPoint implements IShape {

    // ----- STATIC MEMBERS -----
    // ----- DATA MEMEBERS -----
    double x;
    double y;

    // ----- CONSTRUCTORS -----
    public gPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    // ----- DATA ACCESSORS -----
    
    // ----- METHODS -----
    @Override
    public String getWKT() {
        return "POINT ( " + x + " " + y + ")";
    }
}
