/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utils;

/**
 *
 * @author tioannid
 */
public class Utils {

    // ----- STATIC MEMBERS -----
    static public String prettyPrint(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            sb.append(args[i]);
            sb.append(" ");
        }
        return sb.toString();
    }

    // ----- DATA MEMEBERS -----
    // ----- CONSTRUCTORS -----
    // ----- DATA ACCESSORS -----
    // ----- METHODS -----
}
