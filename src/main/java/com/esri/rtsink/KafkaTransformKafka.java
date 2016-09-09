/*
    Consumes a Kafka Topic and applies a transformation (implementation of Transform Interface) to the line from Kafka then prints to Stdout the transformed line.

    Sample implementation of Transform interface TransformSimFile, TransformGeotagSimFile

 */
package com.esri.rtsink;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 *
 * @author david
 */
public class KafkaTransformKafka {

    String brokers;
    String topic;
    String group;
    String topicOut;
    Integer webport;
    Integer timeout;  //ms
    Integer pollingInterval; //ms


    WebServer server;
    KafkaConsumer<String, String> consumer;

    Producer<String, String> producer;

    public KafkaTransformKafka(String brokers, String topic, String group, String topicOut, Integer webport) {
        // Default to 5 second timeout and 10ms polling
        this(brokers,topic, group, topicOut, webport, 5000, 10);
    }


    public KafkaTransformKafka(String brokers, String topic, String group, String topicOut, Integer webport, Integer timeout, Integer pollingInterval) {
        this.brokers = brokers;
        this.topic = topic;
        this.group = group;
        this.topicOut = topicOut;
        this.webport = webport;
        this.timeout = timeout;
        this.pollingInterval = pollingInterval;
        

        try {
        
            Properties propsCons = new Properties();
            propsCons.put("bootstrap.servers",this.brokers);
            // I should include another parameter for group.id this would allow differenct consumers of same topic
            propsCons.put("group.id", this.group);
            propsCons.put("enable.auto.commit", "true");
            propsCons.put("auto.commit.interval.ms", 1000);
            propsCons.put("auto.offset.reset", "earliest");
            propsCons.put("session.timeout.ms", "30000");
            propsCons.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            propsCons.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            
            consumer = new KafkaConsumer<>(propsCons);

            Properties propsProd = new Properties();
            propsProd.put("bootstrap.servers",brokers);
            propsProd.put("client.id", KafkaTransformKafka.class.getName());
            propsProd.put("acks", "1");
            propsProd.put("retries", 0);
            propsProd.put("batch.size", 16384);
            propsProd.put("linger.ms", 1);
            propsProd.put("buffer.memory", 8192000);
            propsProd.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            propsProd.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(propsProd);

            server = new WebServer(this.webport);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    public void read(String transformId) throws Exception {
        
        //Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        consumer.subscribe(Arrays.asList(this.topic));

        Transform transformer = null;

        Class<?> clazz = Class.forName(transformId);
        transformer = (Transform) clazz.newInstance();

//        if (transformId.equalsIgnoreCase("faa-stream")) {
//            transformer = new TransformFaaStream();
//        } else if (transformId.equalsIgnoreCase("simFile")) {
//            transformer = new TransformSimFile();
//        } else if (transformId.equalsIgnoreCase("geotagSimFile")) {
//            transformer = new TransformGeotagSimFile();
//        }
//
        if (transformer == null) throw new Exception("Transformmer not defined");
        
        Long lr = System.currentTimeMillis();
        Long st = System.currentTimeMillis();


        
        Long cnt = 0L;
        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(this.pollingInterval);
            // polls every 100ms
            Long ct = System.currentTimeMillis();
            
            if (cnt > 0 && ct - lr > this.timeout) {
                // Longer than 2 seconds reset and output stats
                
                long delta = lr - st;
                double rate = 1000.0 * (double) cnt / (double) delta;
                System.out.println(cnt + "," + rate);
                

                server.addCnt(cnt);
                server.addRate(rate);
                cnt = 0L;
            }
            
            for (ConsumerRecord<String, String> record : records) {   
                lr = System.currentTimeMillis();
                
                String lineOut = transformer.transform(record.value());



                // Only print a message every 1000 times
                if (!lineOut.isEmpty()) {
                    //System.out.println(cnt + ">> " + record.key() + ":" + lineOut);
                    producer.send(new ProducerRecord<String,String>(this.topicOut, record.key(),lineOut));

                }

                //System.out.println(cnt + ">> " + record.key() + ":" + record.value());

                cnt += 1;      
                if (cnt == 1) {
                    st = System.currentTimeMillis();
                }
            }

        }
    }

    public static void main(String args[]) throws Exception {
        
        // Example Arguments: a1:9092 simFile group1 simFile 100 14002
        
        int numArgs = args.length;
        
        if (numArgs != 6 && numArgs != 8) {
            System.err.print("Usage: KafkaTransformKafka <broker-list-or-kafka-app-name> <topic> <group-id> <transform-class-name> <topicOut> <web-port> (<timeout-ms> <polling-interval-ms>)\n");
        } else {
            
            
            String brokers = args[0];
            String topic = args[1];
            String groupId = args[2];
            String transformId = args[3];
            String topicOut = args[4];
            Integer webport = Integer.parseInt(args[5]);
           
            
            Integer timeout = null;
            Integer pollingInterval = null;
            
            if (numArgs == 8) {
                timeout = Integer.parseInt(args[6]);
                pollingInterval = Integer.parseInt(args[7]);
            }
            
            
            String brokerSplit[] = brokers.split(":");
            
            if (brokerSplit.length == 1) {
                // Try hub name. Name cannot have a ':' and brokers must have it.
                brokers = new MarathonInfo().getBrokers(brokers);
            }   // Otherwise assume it's brokers 
                        
            
            KafkaTransformKafka t = null;
            
            
            System.out.println(brokers);
            System.out.println(topic);
            System.out.println(groupId);
            System.out.println(topicOut);
            System.out.println(webport);
            System.out.println(transformId);

            switch (numArgs) {
                case 6:
                    t = new KafkaTransformKafka(brokers,topic,groupId,topicOut,webport);
                    break;
                case 8:
                    t = new KafkaTransformKafka(brokers,topic,groupId,topicOut,webport,timeout,pollingInterval);
            }
            
            t.read(transformId);
        }
        
        

        
    }    


    
}
