/*
CsvJson parser for simFile

Converts Date String to timestamp; create JsonGeom from lon,lat

 */
package com.esri.rtsink;

import com.esri.core.geometry.OperatorExportToJson;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;

/**
 *
 * @author david
 */
public class TransformSimFile implements Transform {
    
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    @Override
    public String transform(String line) {
        
        if (line == null) return null;
        
        String lineOut = "";
           
        // Load mapping here could come from config file or from web service
                     
        try {
            
            ArrayList<String> vals = new ArrayList<String>();
            Matcher matcher = PATTERN.matcher(line); 
            int i = 0;
            while (matcher.find()) {
                if (matcher.group(2) != null) {
                    //System.out.print(matcher.group(2) + "|");
                    vals.add(i, matcher.group(2));
                } else if (matcher.group(3) != null) {
                    //System.out.print(matcher.group(3) + "|");
                    vals.add(i, matcher.group(3));
                }
                i += 1;
            }     
            

            // This section is hard-coded for simFile format 
            // 1468935966122,138,19-Jul-2016 08:46:06.006,IAH-IAD,-88.368,34.02488,238.75427650928157,57.53489

            Long tm = Long.parseLong(vals.get(0));  // milliseconds from epoch
            
            Integer id = Integer.parseInt(vals.get(1));  // Unique route id Integer
            
            String dtg = vals.get(2);   // Read in Date Time Group String 
            
            String rt = vals.get(3);  // Route <Source Airport Code>-<Destination Airport Code>
            
            

            JSONObject attrJson = new JSONObject();
            attrJson.put("tm", tm);
            attrJson.put("id", id);
            attrJson.put("dtg", dtg);
            attrJson.put("rt", rt);

            // Turn lon/lat into GeoJson
            Double lon = Double.parseDouble(vals.get(4));
            Double lat = Double.parseDouble(vals.get(5));

            Point pt = new Point(lon, lat);

            SpatialReference sr = SpatialReference.create(4326);

            String geomJsonString = OperatorExportToJson.local().execute(sr, pt);            
            JSONObject geomJson = new JSONObject(geomJsonString);

            Double speed = Double.parseDouble(vals.get(6));
            Double bearing = Double.parseDouble(vals.get(7));
            
            attrJson.put("spd", speed);  // speed
            attrJson.put("brg", bearing);  // bearing
            
            // Combine the attributes and the geom
            JSONObject combined = new JSONObject();
            combined.put("attr", attrJson);
            combined.put("geom", geomJson);
            
            lineOut = combined.toString();
            

            
        } catch (Exception e) {
            lineOut = "{\"error\":\"" + e.getMessage() + "\"";
        }
        
        
        return lineOut;
    }
    
    @Override
    public String transform(String line, boolean filter) {
        return transform(line);
    }    
    
}
