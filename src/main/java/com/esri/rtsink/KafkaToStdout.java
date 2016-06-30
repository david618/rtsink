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
import org.json.JSONObject;

/**
 *
 * @author david
 */
public class KafkaToStdout {
    
    String brokers;
    String topic;
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    KafkaConsumer<String, String> consumer;    

    public KafkaToStdout(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
        
        
        
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    static private String parseCsvLine(String line) {
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
    
    
    public void read() {
        
        Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        
        consumer.subscribe(Arrays.asList(this.topic));
        
        
        Long st = System.currentTimeMillis();
        
        Long cnt = 0L;
        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            Long ct = System.currentTimeMillis();
            if (ct - st > 10000) {
                // Longer than 10 seconds reset
                st = ct;
                cnt = 0L;
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
            }
            //break;
        }
    }

    public static void main(String args[]) {
        
        
        if (args.length != 2) {
            System.err.print("Usage: rtsink <broker-list> <topic>\n");
        } else {
            KafkaToStdout t = new KafkaToStdout(args[0], args[1]);
            t.read();
        }
        
        //KafkaToStdout t = new KafkaToStdout("d1.trinity.dev:9092", "faa-stream");
        //t.read();
        

        
    }    
    
}
