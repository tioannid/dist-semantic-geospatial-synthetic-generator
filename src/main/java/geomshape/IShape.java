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
public interface IShape {

    // ----- CONSTANTS
    final double COS30 = Math.sqrt(3) / 2;

    // ----- DATA ACCESSORS -----
    // ----- METHODS -----
    String getWKT();

}
