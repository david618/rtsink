package com.esri.rtsink.scala

import com.esri.rtsink.{MarathonInfo, WebServer}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Created by david on 8/30/16.
  */
object KafkaSparkCnt {
  def main(args: Array[String]) {

    println("START")

    SinkLogging.setStreamingLogLevels()

    val appName = "KafkaSparkCnt"

    val numargs = args.length

    // example: a1:9092 simFile2 group3 a2:9200 elasticsearch simulator simfile2 1 14004

    if (numargs != 4) {
      System.err.println("Usage: KafkaSparkCnt <kafka-brokers-or-hub> <kafka-topic> <kafka-client-group-id> <emitInterval-sec> <webport>")
      System.err.println("        kafka-brokers: Kafka Broker server port, e.g. localhost:9092")
      System.err.println("          kafka-topic: Kafka Topic")
      System.err.println("kafka-client-group-id: Kafka Client Group ID")
      System.err.println("         emitInterval: Number of seconds for spark streaming sampling")
      System.err.println("              webport: Counts port")
      System.exit(1)
    }

    // Set two config files (started with copy of templates)
    //   spark-defaults.conf --> Added line at end of file:  spark.executor.uri http://m1/apps/spark.tgz
    //   spark-env.sh --> Added line at end of file: export JAVA_HOME=/opt/mesosphere/active/java/usr/java

    // spark-submit --class com.esri.rtsink.scala.KafkaSparkCnt --master local[4] rtsink-jar-with-dependencies.jar a1:9092 simFile2 group3 1
    // /root/spark/bin/spark-submit --class com.esri.rtsink.scala.KafkaSparkCnt --master mesos://m1:5050 rtsink-jar-with-dependencies.jar a1:9092 simFile2 group3 1
    /* This ran on both slaves; however, only one was doing all the work.  Topic partitions set at 1.

    $ /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic simFile2 --partitions 2

    $ java -cp Simulator-jar-with-dependencies.jar com.esri.simulator.Kafka a1:9092 simFile2 ../simFile_1000_10s.json 800000 8000000

    Even with two partitions the load still seems to be all carried on one node.

     */


    val Array(brokers,topic,groupid,emitInterval,webport) = args


    println(brokers)
    var kafkaBrokers = brokers
    val brokerSplit = brokers.split(":")
    if (brokerSplit.length == 1) {
      kafkaBrokers = new MarathonInfo().getBrokers(brokers)
    }
    println("brokers: " + kafkaBrokers)

    val port = Integer.parseInt(webport)

    val server = new WebServer(port)

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    case class simfile(rt: String, dtg: String, lon: Double, lat: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(emitInterval.toInt))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> kafkaBrokers,"group.id" -> groupid)

    val ds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )

    var st = System.currentTimeMillis()
    var lr = System.currentTimeMillis()

    var totalNum = 0L

    ds.foreachRDD((rdd: RDD[(String,String)], time: Time) => {
      //rdd.saveAsTextFile("t" + System.currentTimeMillis())
      if (rdd.count() > 0 && totalNum == 0) {
        st = System.currentTimeMillis();
      }
      val jsonRDD = rdd.map(_._2)

      if (rdd.count() > 0) {
        totalNum += rdd.count()
        println()
        println("Time %s: Count %s total records counted)".format(time, rdd.count()))
        lr = System.currentTimeMillis()
      } else {
        //println("Time %s ".format(time))
        if (totalNum > 0) {
          val delta = lr - st
          val rate = 1000.0 * totalNum / delta.toDouble
          println("Total Records Counted in test was %s at a rate of %s".format(totalNum, rate))
          server.addCnt(totalNum)
          server.addRate(rate)
          server.setTm(System.currentTimeMillis())
          totalNum = 0
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

    println("END")
  }

}
