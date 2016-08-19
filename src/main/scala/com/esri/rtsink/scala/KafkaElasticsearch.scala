package com.esri.rtsink.scala


import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * This Object listens to a Kafka topic and writes the CSV file to Elasticsearch
  * The items are expected in simFile format
  */
object CsvKafkaElasticsearch {
  def main(args: Array[String]) {

    println("START")

    SinkLogging.setStreamingLogLevels()

    val appName = "CsvKafkaElasticsearch"

    val numargs = args.length

    // example: a1:9092 simFile group3 a2:9200 elasticsearch simulator simfile

    if (numargs != 7) {
      System.err.println("Usage: CsvKafkaElasticsearch <kafka-brokers> <kafka-topic> <kafka-client-group-id> <esNodes> <clusterName> <indexName> <typeName>")
      System.err.println("        kafka-brokers: Kafka Broker server port, e.g. localhost:9092")
      System.err.println("          kafka-topic: Kafka Topic")
      System.err.println("kafka-client-group-id: Kafka Client Group ID")
      System.err.println("            esNode(s): elasticsearth server port, e.g. localhost:9200")
      System.err.println("          clusterName: Elasticsearch Cluster Name")
      System.err.println("            indexName: Index name for Elasticsearch, e.g. simulator")
      System.err.println("             typeName: Type name for Elasticsearch, e.g. simfile")
      System.exit(1)
    }


    val Array(brokers,topic,groupid,esNodes,esClusterName,esIndexName,esTypeName) = args

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    sparkConf.set("es.index.auto.create", "true").set("es.cluster.name",esClusterName).set("es.nodes", esNodes)

    case class simfile(rt: String, dtg: String, lon: Double, lat: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> groupid)

    val ds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )

    var st = System.currentTimeMillis()


    var totalNum = 0L
    var numSample = 0
    var sumRates = 0.0

    ds.foreachRDD((rdd: RDD[(String,String)], time: Time) => {
      //rdd.saveAsTextFile("t" + System.currentTimeMillis())
      if (rdd.count() > 0) {
        st = System.currentTimeMillis();
      }
      val jsonRDD = rdd.map(_._2).map(_.split(",")).map(e => simfile(e(0), e(2),e(4).toDouble,e(5).toDouble))
      EsSpark.saveToEs(jsonRDD, esIndexName + "/" + esTypeName)
      val delta = System.currentTimeMillis() - st
      val rate = 1000.0 * rdd.count.toDouble / delta.toDouble
      if (rdd.count() > 0) {
        totalNum += rdd.count()
        sumRates += rate
        numSample += 1
        println()
        println("Time %s: Elasticsearch sink (saved %s total records at rate of %s)".format(time, rdd.count(), rate))
      } else {
        //println("Time %s ".format(time))
        if (totalNum > 0) {
          println("Total Records Sent in test was %s written to Elastic at an average rate of %S".format(totalNum, sumRates/numSample))
          totalNum = 0
          numSample = 0
          sumRates = 0.0
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

    println("END")
  }
}


object JsonKafkaElasticsearch {
  def main(args: Array[String]) {

    println("START")

    SinkLogging.setStreamingLogLevels()

    val appName = "JsonKafkaElasticsearch"

    val numargs = args.length

    // example: a1:9092 simFile2 group3 a2:9200 elasticsearch simulator simfile2

    if (numargs != 7) {
      System.err.println("Usage: JsonKafkaElasticsearch <kafka-brokers> <kafka-topic> <kafka-client-group-id> <esNodes> <clusterName> <indexName> <typeName>")
      System.err.println("        kafka-brokers: Kafka Broker server port, e.g. localhost:9092")
      System.err.println("          kafka-topic: Kafka Topic")
      System.err.println("kafka-client-group-id: Kafka Client Group ID")
      System.err.println("            esNode(s): elasticsearth server port, e.g. localhost:9200")
      System.err.println("          clusterName: Elasticsearch Cluster Name")
      System.err.println("            indexName: Index name for Elasticsearch, e.g. simulator")
      System.err.println("             typeName: Type name for Elasticsearch, e.g. simfile")
      System.exit(1)
    }

    // java -cp Simulator-jar-with-dependencies.jar com.esri.simulator.Kafka 10.32.0.15:9476 simFile2 simFile_1000_10s.json 10 100

    // spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master local[8] rtsink-jar-with-dependencies.jar a1:9092 simFile2 group3 a2:9200 elasticsearch simulator simfile2

    // spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master local[8] rtsink-jar-with-dependencies.jar 10.32.0.15:9476 simFile2 group3 10.32.0.20:1025 mesos-ha simulator simfile2

    val Array(brokers,topic,groupid,esNodes,esClusterName,esIndexName,esTypeName) = args

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    sparkConf.set("es.index.auto.create", "true").set("es.cluster.name",esClusterName).set("es.nodes", esNodes)

    case class simfile(rt: String, dtg: String, lon: Double, lat: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers,"group.id" -> groupid)

    val ds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )

    var st = System.currentTimeMillis()


    var totalNum = 0L
    var numSample = 0
    var sumRates = 0.0

    ds.foreachRDD((rdd: RDD[(String,String)], time: Time) => {
      //rdd.saveAsTextFile("t" + System.currentTimeMillis())
      if (rdd.count() > 0) {
        st = System.currentTimeMillis();
      }
      val jsonRDD = rdd.map(_._2)
      EsSpark.saveJsonToEs(jsonRDD, esIndexName + "/" + esTypeName)
      val delta = System.currentTimeMillis() - st
      val rate = 1000.0 * rdd.count.toDouble / delta.toDouble
      if (rdd.count() > 0) {
        totalNum += rdd.count()
        sumRates += rate
        numSample += 1
        println()
        println("Time %s: Elasticsearch sink (saved %s total records at rate of %s)".format(time, rdd.count(), rate))
      } else {
        //println("Time %s ".format(time))
        if (totalNum > 0) {
          println("Total Records Sent in test was %s written to Elastic at an average rate of %S".format(totalNum, sumRates/numSample))
          totalNum = 0
          numSample = 0
          sumRates = 0.0
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

    println("END")
  }
}
