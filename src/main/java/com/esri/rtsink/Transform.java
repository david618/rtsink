/*

Interface for parsing CSV to JSON

Input
Line of Text (csv, json, xml)


Output 
Line of Text (csv, json, xml)


 */
package com.esri.rtsink;
/**
 *
 * @author david
 */
public interface Transform {
    public String transform(String line);

}
