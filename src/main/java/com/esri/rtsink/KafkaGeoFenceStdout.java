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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class KafkaGeoFenceStdout {

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
    
    
    public KafkaGeoFenceStdout(String brokers, String topic, String group, String fenceUrl, String fieldName, Integer webport) {
        this.brokers = brokers;
        this.topic = topic;
        this.group = group;
        this.fenceUrl = fenceUrl;
        this.fieldName = fieldName;
        this.webport = webport;

        try {

            Properties props = new Properties();
            props.put("bootstrap.servers", this.brokers);
            // I should include another parameter for group.id this would allow differenct consumers of same topic
            props.put("group.id", this.group);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", 1000);
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);

            server = new WebServer(this.webport);
            
            loadFences(this.fenceUrl, this.fieldName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public KafkaGeoFenceStdout() {
    }

    public String parseCsvLine(String line) {
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
            
            attrJson.put("geotag", geoTag);

            // Combine the attributes and the geom
            JSONObject combined = new JSONObject();
            combined.put("attr", attrJson);
            combined.put("geom", geomJson);

            lineOut = combined.toString();
            
//            if (!geoTag.isEmpty()) {
//                System.out.println(lineOut + "\n");
//            }
            

        } catch (Exception e) {
            lineOut = "{\"error\":\"" + e.getMessage() + "\"";
        }

        return lineOut;
    }

    public void read() {

        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        consumer.subscribe(Arrays.asList(this.topic));

        Long lr = System.currentTimeMillis();
        Long st = System.currentTimeMillis();

        Long cnt = 0L;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            // polls every 100ms
            Long ct = System.currentTimeMillis();

            if (cnt > 0 && ct - lr > 2000) {
                // Longer than 2 seconds reset and output stats

                long delta = lr - st;
                double rate = 1000.0 * (double) cnt / (double) delta;
                System.out.println(cnt + "," + rate);

                cnt = 0L;
                server.setCnt(cnt);

            }
            for (ConsumerRecord<String, String> record : records) {
                lr = System.currentTimeMillis();
//                if (cnt%1000 == 0) {    
                    // Only print a message every 1000 times
                    String lineOut = parseCsvLine(record.value());
//                    System.out.println(cnt + ">> " + record.key() + ":" + lineOut);
                    //System.out.println(cnt + ">> " + record.key() + ":" + record.value());
//                }
                cnt += 1;
                if (cnt == 1) {
                    st = System.currentTimeMillis();
                }
            }
            server.setCnt(cnt);
            
            

        }
    }
    

    public void showFences() {
        
        try {
            
            Iterator it = this.polysMap.entrySet().iterator();
            
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.println(pair.getKey() + " = " + pair.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
  
    
    public String getNamesWithin(Geometry pt) {
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
    

    public boolean loadFences(String url, String fieldName) {
        // Hard coded for now to read fences from a local file

        //String fieldName = "iata_faa";

        boolean isLoaded = false;
        
//        String fenceJsonFile = "airports1000FS.json";                
//        FileReader fr = null;
        
        //String url = "http://m1.trinity.dev/airports1000FS.json";
        InputStream is = null;
        BufferedReader rd = null;
        
                        
        try {

//            fr = new FileReader(fenceJsonFile);
//            JSONTokener tokener = new JSONTokener(fr);
//            JSONObject root = new JSONObject(tokener);
            
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
                
                //ArrayList<MapGeometry> polys = new ArrayList<>();
                
                while (i < cnt) {
//                    keyInterator = features.getJSONObject(i).keys();
//
//                    while (keyInterator.hasNext()) {
//                        System.out.println(keyInterator.next());
//                    }
//                    
//                    JSONObject attributes = features.getJSONObject(i).getJSONObject("attributes");
//                    System.out.println("<< attributes >>");
//                    
//                    keyInterator = attributes.keys();
//
//                    while (keyInterator.hasNext()) {
//                        System.out.println(keyInterator.next());
//                    }
//                    
//                    JSONObject geom = features.getJSONObject(i).getJSONObject("geometry");
//                    System.out.println(geom.toString());
//                    
//                    
//                    
//                    i += 1;
//                    break;
                      
                    JSONObject geom = features.getJSONObject(i).getJSONObject("geometry");
                    
                    
                    MapGeometry poly =  OperatorImportFromJson.local().execute(Geometry.Type.Polygon, geom);
                    
                    String attr = features.getJSONObject(i).getJSONObject("attributes").getString(fieldName);
                    
                    //polys.add(poly);
                    this.polysMap.put(attr, poly.getGeometry());
                    

//                    if (OperatorWithin.local().execute(pt, poly.getGeometry(), sr, null)) { 
//                        System.out.println("In Fence");
//                        System.out.println(geom.toString());
//                    }
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

    public static void main(String args[]) throws Exception {

        // Example params: d1.trinity.dev:9092 simFile group1 http://m1.trinity.dev/airports1000FS.json iata_faa 9001
        
        // d1.trinity.dev:9092 simFile group1 http://m1.trinity.dev/airports1000FS.json iata_faa 9001
        
        if (args.length != 6) {
            System.err.print("Usage: KafkaGeoFenceStdout <broker-list> <topic> <group-id> <url-esri-json-fences> <geotag-field> <web-port>\n");
        } else {
            
            String brokers = args[0];
            
            String brokerSplit[] = brokers.split(":");
            
            if (brokerSplit.length == 1) {
                // Try hub name. Name cannot have a ':' and brokers must have it.
                brokers = new MarathonInfo().getBrokers(brokers);
            }   // Otherwise assume it's brokers 
                     
            
            KafkaGeoFenceStdout t = new KafkaGeoFenceStdout(args[0], args[1], args[2], args[3], args[4], Integer.parseInt(args[5]));

            t.read();
        }

//            KafkaGeoFenceStdout t = new KafkaGeoFenceStdout();
//
//            t.loadFences();
//            t.showFences();
    }

}
