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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 *
 * @author david
 */
public class KafkaToStdout {
    
    String brokers;
    String topic;
    
    KafkaConsumer<String, String> consumer;    

    public KafkaToStdout(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
        
        try {
        
            Properties props = new Properties();
            props.put("bootstrap.servers",this.brokers);
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
                    System.out.println(cnt + ">> " + record.key() + ":" + record.value());
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
