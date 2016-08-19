package com.esri.rtsink.scala


import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by david on 8/19/16.
  */
object CsvKafkaElasticsearch {
  def main(args: Array[String]) {

    println("START")

    SinkLogging.setStreamingLogLevels()

    val appName = "CsvSparkElasticsearch"

    val numargs = args.length

    // example: a3:9200 elasticsearch simulator simfile /home/david/streamfiles

    if (numargs != 5) {
      System.err.println("Usage: CsvKafkaElasticsearch <kafka-brokers> <kafka-topic> <esNodes> <clusterName> <indexName> <typeName>")
      System.err.println("        kafkabroker(s): Kafka Broker server port, e.g. localhost:9092")
      System.err.println("        kafkaTopic(s): Kafka Topic")
      System.err.println("            esNode(s): elasticsearth server port, e.g. localhost:9200")
      System.err.println("          clusterName: Elasticsearch Cluster Name")
      System.err.println("            indexName: Index name for Elasticsearch, e.g. simulator")
      System.err.println("             typeName: Type name for Elasticsearch, e.g. simfile")
      System.exit(1)
    }
    // spark-submit --class com.esri.simulator.scala.CsvElasticsearch --master local[8] Simulator-jar-with-dependencies.jar a3:9200 elasticsearch simulator simfile /home/david/streamfiles


    val Array(brokers,topics,esNodes,esClusterName,esIndexName,esTypeName) = args

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    sparkConf.set("es.index.auto.create", "true").set("es.cluster.name",esClusterName).set("es.nodes", esNodes)

    case class simfile(rt: String, dtg: String, lon: Double, lat: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)

    val ds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )

    var st = System.currentTimeMillis()



    ds.foreachRDD((rdd: RDD[(String,String)], time: Time) => {
      rdd.saveAsTextFile("test123")

      //EsSpark.saveToEs(rdd, esIndexName + "/" + esTypeName)
    })


//    ds.foreachRDD((rdd: RDD[String], time: Time) => {
//      if (rdd.count() > 0) {
//        st = System.currentTimeMillis();
//      }
//      val jsonRDD = rdd.map(_.split(",")).map(e => simfile(e(0), e(2),e(4).toDouble,e(5).toDouble))
//      EsSpark.saveToEs(jsonRDD, esIndexName + "/" + esTypeName)
//      val delta = System.currentTimeMillis() - st
//      val rate = 1000.0 * rdd.count.toDouble / delta.toDouble
//      if (rdd.count() > 0) {
//        println()
//        println("Time %s: Elasticsearch sink (saved %s total records at rate of %s)".format(time, rdd.count(), rate))
//      } else {
//        //println("Time %s ".format(time))
//      }
//    })

    ssc.start()
    ssc.awaitTermination()

    println("END")
  }
}
