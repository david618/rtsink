/*
CsvJson parser for faa-stream.
 */
package com.esri.rtsink;

import com.esri.core.geometry.OperatorExportToJson;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;

/**
 *
 * @author david
 */
public class TransformFaaStream implements Transform {
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    @Override
    public String transform(String line) {
        String lineOut = "";
           
        // Load mapping here could come from config file or from web service
                     
        try {
            
            /*
                Using PATTERN the parser can handled quoted strings containing commas, For Example
            
                Line from faa-stream.csv
                FAA-Stream,13511116,DLH427,06/22/2013 12:02:00 AM,-55.1166666666667,45.1166666666667,82.660223052638,540,350,A343,DLH,KPHL,EDDF,JET,COMMERCIAL,ACTIVE,GA,"-55.1166666666667,45.1166666666667,350.0"
            */
            
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
            
            // This was hard-coded for faa-stream.csv
            String tn = vals.get(2);
            
            DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss aa");
            Date dt = df.parse(vals.get(3));

            Calendar cal = Calendar.getInstance();
            cal.setTime(dt);
            Long tm = cal.getTimeInMillis();

            JSONObject attrJson = new JSONObject();
            attrJson.put("tn", tn);
            attrJson.put("timestamp", tm);
            

            Double lon = Double.parseDouble(vals.get(4));
            Double lat = Double.parseDouble(vals.get(5));


            Point pt = new Point(lon, lat);

            SpatialReference sr = SpatialReference.create(4326);

            String geomJsonString = OperatorExportToJson.local().execute(sr, pt);            
            JSONObject geomJson = new JSONObject(geomJsonString);
            
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
