/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.esri.rtsink;

import com.esri.core.geometry.OperatorExportToJson;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
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

/**
 *
 * @author david
 */
public class KafkaToStdout {
    
    String brokers;
    String topic;
    Integer webport;
    WebServer server;
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    KafkaConsumer<String, String> consumer;    

    public KafkaToStdout(String brokers, String topic, Integer webport) {
        this.brokers = brokers;
        this.topic = topic;
        this.webport = webport;
        
        
        try {
        
            Properties props = new Properties();
            props.put("bootstrap.servers",this.brokers);
            // I should include another parameter for group.id this would allow differenct consumers of same topic
            props.put("group.id", "test2");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", 1000);
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            
            consumer = new KafkaConsumer<>(props);
            
            server = new WebServer(this.webport);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    static private String parseCsvLine(String line) {
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
            
            /*
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
            */

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
    
    
    public void read() {
        
        Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        
        consumer.subscribe(Arrays.asList(this.topic));
        
        
        Long st = System.currentTimeMillis();
        
        Long cnt = 0L;
        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            // polls every 100ms
            Long ct = System.currentTimeMillis();
            
            if (ct - st > 30000) {
                // Longer than 30 seconds reset
                st = ct;
                cnt = 0L;
                server.setCnt(cnt);
                System.out.println("Reset Cnt");
            }
            for (ConsumerRecord<String, String> record : records) {   
                st = System.currentTimeMillis();
                if (cnt%1000 == 0) {    
                    // Only print a message every 1000 times
                    String lineOut = parseCsvLine(record.value());
                    System.out.println(cnt + ">> " + record.key() + ":" + lineOut);
                    //System.out.println(cnt + ">> " + record.key() + ":" + record.value());
                }
                cnt += 1;
                server.setCnt(cnt);
            }
            //break;
        }
    }

    public static void main(String args[]) {
  
//        try {
//            JSONObject obj = new JSONObject();
//            
//            JSONArray array = new JSONArray();
//
//            
//            array.put(new String("1.2.3.4"));
//            array.put(new String("2.3.3.4"));
//
//            obj.put("ips", array);
//
//            
//            
//            
//            System.out.println(obj.toString());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        
        
        
        if (args.length != 3) {
            System.err.print("Usage: rtsink <broker-list> <topic> <web-port>\n");
        } else {
            KafkaToStdout t = new KafkaToStdout(args[0], args[1], Integer.parseInt(args[2]));
            t.read();
        }
        
        //KafkaToStdout t = new KafkaToStdout("d1.trinity.dev:9092", "faa-stream");
        //t.read();
        

        
    }    
    
}
