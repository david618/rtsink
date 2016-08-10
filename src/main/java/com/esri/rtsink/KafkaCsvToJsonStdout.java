/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.esri.rtsink;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 *
 * @author david
 */
public class KafkaCsvToJsonStdout {
    
    String brokers;
    String topic;
    String group;
    Integer webport;
    WebServer server;
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    KafkaConsumer<String, String> consumer;    

    public KafkaCsvToJsonStdout(String brokers, String topic, String group, Integer webport) {
        this.brokers = brokers;
        this.topic = topic;
        this.group = group;
        this.webport = webport;
        
        
        
        try {
        
            Properties props = new Properties();
            props.put("bootstrap.servers",this.brokers);
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
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    public void read(String topic, int everyNthLine) {
        
        Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        
        consumer.subscribe(Arrays.asList(this.topic));
        
        
        CsvJsonParser parser = null;
        if (topic.equalsIgnoreCase("faa-stream")) {
            parser = new CsvJsonFaaStream();
        } else if (topic.equalsIgnoreCase("simFile")) {
            parser = new CsvJsonSimFile();
        }                              
        
        Long lr = System.currentTimeMillis();
        Long st = System.currentTimeMillis();
        
        Long cnt = 0L;
        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(10);
            // polls every 100ms
            Long ct = System.currentTimeMillis();
            
            if (cnt > 0 && ct - lr > 2000) {
                // Longer than 2 seconds reset and output stats
                
                long delta = lr - st;
                double rate = 1000.0 * (double) cnt / (double) delta;
                System.out.println(cnt + "," + rate);
                
                cnt = 0L;
                //server.setCnt(cnt);
                
            }
            
            for (ConsumerRecord<String, String> record : records) {   
                lr = System.currentTimeMillis();
                String lineOut = parser.parseCsvLine(record.value());
                if (cnt%everyNthLine == 0) {    
                    // Only print a message every 1000 times                    
                    System.out.println(cnt + ">> " + record.key() + ":" + lineOut);
                    //System.out.println(cnt + ">> " + record.key() + ":" + record.value());
                }
                cnt += 1;      
                if (cnt == 1) {
                    st = System.currentTimeMillis();
                }
            }
            server.setCnt(cnt);
            
        }
    }

    public static void main(String args[]) throws Exception {
        
        // Example Arguments: 
        
        if (args.length != 5) {
            System.err.print("Usage: KafkaStdout <broker-list> <topic> <group-id> <every-nth-line> <web-port>\n");
        } else {
            
            String brokers = args[0];
            
            String brokerSplit[] = brokers.split(":");
            
            if (brokerSplit.length == 1) {
                // Try hub name. Name cannot have a ':' and brokers must have it.
                brokers = new MarathonInfo().getBrokers(brokers);
            }   // Otherwise assume it's brokers 
                        
            
            KafkaCsvToJsonStdout t = new KafkaCsvToJsonStdout(brokers, args[1], args[2], Integer.parseInt(args[4]));
            t.read(args[1], Integer.parseInt(args[3]));
        }
        
        

        
    }    
    
}
