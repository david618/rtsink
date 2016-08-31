/*

    Spark client (in Java).  Consumes Kafka topic and counts records.

 */
package com.esri.rtsink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 *
 * @author david
 */
public class KafkaSparkCnt {
    
    
    WebServer server;
    Long cnt = 0L;

    // Last read and start time
    private Long lr = System.currentTimeMillis();
    private Long st = System.currentTimeMillis();    
    

    public void startWebServer(Integer webport) {
        server = new WebServer(webport);
    }

    public void updateCnt(Long num) {
        
                
        Long ct = System.currentTimeMillis();    
        
        if (num > 0) {
            
            if (this.cnt == 0) {
                // This is the first batch read start timer
                st = System.currentTimeMillis();    
            }
            
            this.cnt += num;
            lr = System.currentTimeMillis();            
        }
        
        if (this.cnt > 0 && ct - this.lr > 2000) {
            // more than two seconds since an update; reset
            long delta = lr - st;
            double rate = 1000.0 * (double) cnt / (double) delta;
            System.out.println(cnt + "," + rate);
            
            this.cnt = 0L;
            
        }
        
        server.addCnt(this.cnt);
        
        
    }
    
    
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String args[]) throws Exception {

        /*
          /opt/spark/bin/spark-submit --class com.esri.rtsink.KafkaSparkCnt
        --master mesos://spark-dispatcher.marathon.mesos:7077
        --deploy-mode cluster http://m1.trinity.dev/0.10.0.0/rtsink-jar-with-dependencies.jar 
        d1.trinity.dev:9092 simFile group1 9002        
        
        /opt/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master mesos://master.mesos:5050 spark-examples_2.11-1.6.1.jar 100 3
        
        
        /opt/spark/bin/spark-submit --class com.esri.rtsink.KafkaSparkCnt
        --master mesos://master.mesos:5050 rtsink-jar-with-dependencies.jar 
        d1.trinity.dev:9092 simFile group2 9002      
        
        Marathon:        
        CMD: $MESOS_SANDBOX/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi --master local[4] $MESOS_SANDBOX/spark-examples_2.11-1.6.1.jar 100 3; tail -f /var/log/messages 
        URIs:  http://master.mesos/spark-examples.tgz, http://master.mesos/spark.tgz
        
        spark.tgz was customized with executor uri set 
        spark.executor.uri http://m1.trinity.dev/spark-1.6.1-bin-hadoop2.6_2.11.tgz
        
        */
        
        
        // Example parameters: d1.trinity.dev:9092 simFile group1 9002
        if (args.length != 4) {
            System.err.print("Usage: KafkaSparkCnt <broker-list> <topic> <group-id> <web-port>\n");
            throw new Exception("Invalid Parameters");
        }        

        String brokers = args[0];
        String topics = args[1];
        String groupId = args[2];        
            
        String brokerSplit[] = brokers.split(":");

        if (brokerSplit.length == 1) {
            // Try hub name. Name cannot have a ':' and brokers must have it.
            brokers = new MarathonInfo().getBrokers(brokers);
        }   // Otherwise assume it's brokers         
        
        Integer webport = 0;
        try {
            webport = Integer.parseInt(args[3]);
        } catch (Exception e) {
            throw new Exception("The web-port must be an integer");
        }
        
        if (webport < 1024 || webport > 65535) {
            throw new Exception("The web-port must be greater than 1024 and less than 65535");
        }


        String appName = "KafkaSparkParser";


        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Logger.getLogger("kafka").setLevel(Level.ERROR);
        
        final KafkaSparkCnt kss = new KafkaSparkCnt();
        kss.startWebServer(webport);
        

        SparkConf sparkConf = new SparkConf().setAppName(appName);
        // Uncomment the following line for debugging in IDE
        //sparkConf.setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(100L));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupId);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        lines.foreachRDD(
                new Function2<JavaRDD<String>, Time, Void>() {
            @Override
            public Void call(JavaRDD<String> t1, Time t2) throws Exception {                        
                Long t1Cnt = t1.count();
                //System.out.println("Processed: " + t1Cnt);
                
                kss.updateCnt(t1Cnt);
                return null;
            }
        });


        jssc.start();
        jssc.awaitTermination();

    }
}
