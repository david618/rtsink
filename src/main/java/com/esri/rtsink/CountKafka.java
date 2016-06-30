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
import org.apache.kafka.common.TopicPartition;

/**
 *
 * @author david
 */
public class CountKafka {
    
    String brokers;
    String topic;
    
    KafkaConsumer<String, String> consumer;

    public CountKafka(String brokers, String topic) {
        this.brokers = brokers;
        this.topic = topic;
        
        try {
        
            Properties props = new Properties();
            props.put("bootstrap.servers",this.brokers);
            props.put("group.id", "test3");
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
    
    
    public void read2() {
        TopicPartition test = new TopicPartition(this.topic, 0);
       
        
    }
    
    public void read() {
        
        Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        
        consumer.subscribe(Arrays.asList(this.topic));
        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + ":" + record.value());                                
            }
            //break;
        }
    }
    
    
    public Integer getCount() {
        
        Integer cnt = 0;
        Integer z = 0;
        try {        
            
            
            
            //consumer.subscribe(Arrays.asList(this.topic));
            

            TopicPartition test = new TopicPartition(this.topic, 0);
            consumer.assign(Arrays.asList(test));
            //consumer.seek(test, 0);

            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(100);
                                                
                int i = 0;
                String t = "";
                for (ConsumerRecord<String, String> record : records) {
                    t = record.topic();
                    i += 1;
                }
                
                
                                
                if (i == 0) z += 1; else z = 0;
                if (z == 10) break;
                
                
                
                cnt += i;
                
            }
                        
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return cnt;

    }
    
    
    public static void main(String args[]) {
        CountKafka t = new CountKafka("d1.trinity.dev:9092", "faa-stream");
        //t.read();
        
        System.out.println(t.getCount());
        //System.out.println(CountKafka.class.getName());
        
    }
    
}
