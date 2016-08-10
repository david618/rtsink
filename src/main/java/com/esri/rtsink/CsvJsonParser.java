/*

Interface for parsing CSV to JSON

Input
json-element:function of csv fields
tn:$1
timestamp:mktimestamp($3, "MM/dd/yyyy HH:mm:ss aa")
geom:mkgeom($4,$5,4326)


 */
package com.esri.rtsink;
/**
 *
 * @author david
 */
public interface CsvJsonParser {
    public String parseCsvLine(String line);
}
