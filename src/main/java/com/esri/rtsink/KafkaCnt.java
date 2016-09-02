/*
 Consume a Kafka topic and count lines as they appear.
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
public class KafkaCnt {
    
    String brokers;
    String topic;
    String group;
    Integer webport;
    boolean calcLatency;

    WebServer server;
    
    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");
    
    KafkaConsumer<String, String> consumer;    

    public KafkaCnt(String brokers, String topic, String group, Integer webport, boolean calcLatency) {
        this.brokers = brokers;
        this.topic = topic;
        this.group = group;
        this.webport = webport;
        this.calcLatency = calcLatency;
        
        
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
    
    
    public void read() {
        
        Map<String,List<PartitionInfo>> topics = consumer.listTopics();
       
        
        consumer.subscribe(Arrays.asList(this.topic));
        
        
        Long lr = System.currentTimeMillis();
        Long st = System.currentTimeMillis();
        
        Long cnt = 0L;
        Long sumLatencies = 0L;

        boolean calcLaten = this.calcLatency;

        
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(10);
            // polls every 10ms
            Long ct = System.currentTimeMillis();
            
            if (cnt > 0 && ct - lr > 5000) {
                // Longer than 2 seconds reset and output stats
                
                long delta = lr - st;
                double rate = 1000.0 * (double) cnt / (double) delta;

                double avgLatency = (double) sumLatencies / (double) cnt;  // ms

                if (calcLaten) {
                    //System.out.println(cnt + "," + rate + "," + avgLatency);
                    System.out.format("%d , %.0f , %.3f\n", cnt,rate,avgLatency);
                } else {
                    //System.out.println(cnt + "," + rate);
                    System.out.format("%d , %.0f\n", cnt,rate);
                }

                
                server.addRate(rate);
                server.addLatency(avgLatency);
                server.setTm(System.currentTimeMillis());
                server.addCnt(cnt);
                cnt = 0L;
                sumLatencies = 0L;
                calcLaten = this.calcLatency;
                
                
            }
            
            for (ConsumerRecord<String, String> record : records) {   
                lr = System.currentTimeMillis();
                cnt += 1;      
                if (cnt == 1) {
                    st = System.currentTimeMillis();

                }
                if (calcLaten) {
                    try {
                        String line = record.value();
                        long tsent = Long.parseLong(line.substring(line.lastIndexOf(",") + 1));

                        long trcvd = System.currentTimeMillis();

                        sumLatencies += (trcvd - tsent);

                    } catch (Exception e) {
                        System.out.println("For Latency Calculations last field in CSV must be milliseconds from Epoch");
                        calcLaten = false;
                    }


                }


            }
        }
    }

    public static void main(String args[]) throws Exception {
          // Example Command Line Args: a1:9092 simFile group1 9001

        int numargs = args.length;

        if (numargs != 4 && numargs != 5) {
            System.err.print("Usage: rtsink <broker-list-or-hub-name> <topic> <group-id> <web-port> (<calc-latency>)\n");
        } else {
            
            String brokers = args[0];
            
            String brokerSplit[] = brokers.split(":");
            
            if (brokerSplit.length == 1) {
                // Try hub name. Name cannot have a ':' and brokers must have it.
                brokers = new MarathonInfo().getBrokers(brokers);
            }   // Otherwise assume it's brokers 

            KafkaCnt t = null;

            if (numargs == 4) {
                t = new KafkaCnt(brokers, args[1], args[2], Integer.parseInt(args[3]), false);
            } else {
                t = new KafkaCnt(brokers, args[1], args[2], Integer.parseInt(args[3]),Boolean.parseBoolean(args[4]));
            }

            t.read();
        }

        
    }    
    
}
