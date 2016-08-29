/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.esri.rtsink;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.OperatorExportToJson;
import com.esri.core.geometry.OperatorImportFromJson;
import com.esri.core.geometry.OperatorWithin;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 *
 * @author david
 */
public class TransformGeotagSimFile implements Transform {

    String brokers;
    String topic;
    String group;
    String fenceUrl;
    String fieldName;
    Integer webport;
    WebServer server;

    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");

    KafkaConsumer<String, String> consumer;

    private HashMap<String, Geometry> polysMap = new HashMap<>();

    Properties prop = new Properties();
    InputStream input = null;
    
    public TransformGeotagSimFile() throws Exception {

        String filename = "config.properties";
        input = new FileInputStream(this.getClass().getName() + ".properties");
        ///input = TransformGeotagSimFile.class.getClassLoader().getResourceAsStream(filename);
        if(input==null){
            System.out.println("Sorry, unable to find " + filename);
            return;
        }

        //load a properties file from class path, inside static method
        prop.load(input);

        // Change this app so that fenceURL and fieldName come from Environment or Config file

        this.fenceUrl = prop.getProperty("fenceUrl");
        this.fieldName = prop.getProperty("fieldName");

        System.out.println(this.fenceUrl);
        System.out.println(this.fieldName);

        try {        
            loadFences(this.fenceUrl, this.fieldName);

        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new Exception("TEST");
    }

   
    public void showFences() {        
        try {            
            for (Map.Entry pair : this.polysMap.entrySet()) {
                System.out.println(pair.getKey() + " = " + pair.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
  
    
    private String getNamesWithin(Geometry pt) {
        // Given a point check all the fences and return comma sep string of airports
        String fields = "";
        SpatialReference sr = SpatialReference.create(4326);
        
        try {
            Iterator<Map.Entry<String,Geometry>> it = this.polysMap.entrySet().iterator();
            
            while (it.hasNext()) {

                Map.Entry<String,Geometry> pair = (Map.Entry<String,Geometry>) it.next();
                //System.out.println(pair.getKey() + " = " + pair.getValue());
                
                
                if (OperatorWithin.local().execute(pt, pair.getValue(), sr, null)) { 
                    if (fields.isEmpty()) {
                        fields += pair.getKey();
                    } else {
                        fields += "," + pair.getKey();
                    }

                }                
                
            }            
            
        } catch (Exception e ) {
            e.printStackTrace();
        }
       
        return fields;
               
    }
    

    private boolean loadFences(String url, String fieldName) {
        // Hard coded for now to read fences from a local file

        //String fieldName = "iata_faa";

        boolean isLoaded = false;
        
        
        //String url = "http://m1.trinity.dev/airports1000FS.json";
        InputStream is = null;
        BufferedReader rd = null;
        
                        
        try {
           
            is = new URL(url).openStream();
            rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            StringBuilder sb = new StringBuilder();
            int cp;
            while ((cp = rd.read()) != -1) {
                sb.append((char) cp);
            }
            String jsonText = sb.toString();
            
            JSONObject root = new JSONObject(jsonText);
                                   
            Iterator<String> keyInterator = root.keys();
            
            while (keyInterator.hasNext()) {
                System.out.println(keyInterator.next());
            }
            
            if (root.has("features")) {
                
                JSONArray features = root.getJSONArray("features");
                int cnt = features.length();
                System.out.println("File has " + cnt + " Features");
                int i = 0;
                
                
                while (i < cnt) {

                      
                    JSONObject geom = features.getJSONObject(i).getJSONObject("geometry");
                    
                    
                    MapGeometry poly =  OperatorImportFromJson.local().execute(Geometry.Type.Polygon, geom);
                    
                    String attr = features.getJSONObject(i).getJSONObject("attributes").getString(fieldName);
                    
                    //polys.add(poly);
                    this.polysMap.put(attr, poly.getGeometry());
                    

                    i += 1;
                    
                }
                

                while (keyInterator.hasNext()) {
                    System.out.println(keyInterator.next());
                }
                
            }

        } catch (Exception e) {
                        
            e.printStackTrace();
                                    
        } finally {
            //try { fr.close(); } catch (Exception e) { /* ok to ignore*/ }
            try { is.close(); } catch (Exception e) { /* ok to ignore*/ }
            try { rd.close(); } catch (Exception e) { /* ok to ignore*/ }
        }

        return isLoaded;
    }

    
    


    @Override
    public String transform(String line) {
        String outLine = "";
        return transform(line, false);
    }
    
    
    @Override
    public String transform(String line, boolean filter) {
        String lineOut = "";

        // Load mapping here could come from config file or from web service
        try {

            /*
                Using PATTERN the parser can handled quoted strings containing commas, For Example
            
                Line from faa-stream.csv
                FAA-Stream,13511116,DLH427,06/22/2013 12:02:00 AM,-55.1166666666667,45.1166666666667,82.660223052638,540,350,A343,DLH,KPHL,EDDF,JET,COMMERCIAL,ACTIVE,GA,"-55.1166666666667,45.1166666666667,350.0"

                The quoted string "-55.1166666666667,45.1166666666667,350.0" is parsed as a single field.
            
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
            

            String geoTag = getNamesWithin(pt);
            
            if (filter) {
                if (geoTag.isEmpty()) {
                    return "";
                }
            }
            
            attrJson.put("geotag", geoTag);

            // Combine the attributes and the geom
            JSONObject combined = new JSONObject();
            combined.put("attr", attrJson);
            combined.put("geom", geomJson);

            lineOut = combined.toString();
            

        } catch (Exception e) {
            lineOut = "{\"error\":\"" + e.getMessage() + "\"";
        }

        return lineOut;    }
    
    
    public static void main(String args[]) throws Exception {
        
        
        TransformGeotagSimFile t = new TransformGeotagSimFile();
        
        FileReader fr = new FileReader("C:\\Users\\davi5017\\Documents\\NetBeansProjects\\Simulator\\simFile_1000_10s.dat");
        BufferedReader br = new BufferedReader(fr);
        
        
        
        int n = 0;
        while (n < 1000 && br.ready() ) {
            String line = br.readLine();
            
            
            String outLine = t.transform(line, true);
            if (!outLine.isEmpty()) {
                System.out.println(line);
                System.out.println(outLine);
            }
            
            n++;
        }
        
        br.close();
        fr.close();

    }    



}
