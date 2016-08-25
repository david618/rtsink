package com.esri.rtsink.scala


import com.esri.rtsink.MarathonInfo
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, Time}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
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

    if (numargs != 8) {
      System.err.println("Usage: CsvKafkaElasticsearch <kafka-brokers> <kafka-topic> <kafka-client-group-id> <esNodes> <clusterName> <indexName> <typeName>")
      System.err.println("        kafka-brokers: Kafka Broker server port, e.g. localhost:9092")
      System.err.println("          kafka-topic: Kafka Topic")
      System.err.println("kafka-client-group-id: Kafka Client Group ID")
      System.err.println("            esNode(s): elasticsearth server port, e.g. localhost:9200")
      System.err.println("          clusterName: Elasticsearch Cluster Name")
      System.err.println("            indexName: Index name for Elasticsearch, e.g. simulator")
      System.err.println("             typeName: Type name for Elasticsearch, e.g. simfile")
      System.err.println("         emitInterval: Number of seconds for spark streaming sampling")
      System.exit(1)
    }

/*

java -jar Simulator-jar-with-dependencies.jar tcp-kafka.marathon.mesos 5565 simFile_1000_10s.dat 100 1000

spark-submit --conf spark.executor.memory=4g --conf spark.cores.max=16  --class com.esri.rtsink.scala.CsvKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.18:9460 simFile group3 10.32.0.11:1025 mesos-ha sink simfile


spark-submit --conf spark.executor.memory=4g --conf spark.cores.max=16 --conf es.batch.size.bytes=10 --conf es.batch.size.entries=20000  --class com.esri.rtsink.scala.CsvKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.18:9460 simFile group3 10.32.0.11:1025 mesos-ha sink simfile



 */


    val Array(brokers,topic,groupid,esNodes,esClusterName,esIndexName,esTypeName,emitInterval) = args

    println(brokers)
    var kafkaBrokers = brokers
    val brokerSplit = brokers.split(":")
    if (brokerSplit.length == 1) {
      kafkaBrokers = new MarathonInfo().getBrokers(brokers)
    }
    println("brokers: " + kafkaBrokers)

    println(esNodes)
    var elasticNodes = esNodes
    var elasticClusterName = esClusterName
    val nodesSplit = esNodes.split(":")
    if (nodesSplit.length == 1) {
      val mi = new MarathonInfo()
      elasticNodes = mi.getElasticSearchHttpAddresses(esNodes);
      elasticClusterName = mi.getElasticSearchClusterName(esNodes);
    }
    println(elasticNodes)
    println(elasticClusterName)

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    sparkConf.set("es.index.auto.create", "true")
      .set("es.cluster.name",elasticClusterName)
      .set("es.nodes", elasticNodes)

    case class simfile(tm: Long, id: String, dtg: String, rt: String, lon: Double, lat: Double, speed: Double, bearing: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(emitInterval.toInt))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> kafkaBrokers,"group.id" -> groupid)

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
      val jsonRDD = rdd.map(_._2).map(_.split(",")).map(e => simfile(e(0).toLong,e(1),e(2),e(3),e(4).toDouble,e(5).toDouble,e(6).toDouble,e(7).toDouble))



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

    if (numargs != 8) {
      System.err.println("Usage: JsonKafkaElasticsearch <kafka-brokers> <kafka-topic> <kafka-client-group-id> <esNodes> <clusterName> <indexName> <typeName>")
      System.err.println("        kafka-brokers: Kafka Broker server port, e.g. localhost:9092")
      System.err.println("          kafka-topic: Kafka Topic")
      System.err.println("kafka-client-group-id: Kafka Client Group ID")
      System.err.println("            esNode(s): elasticsearth server port, e.g. localhost:9200")
      System.err.println("          clusterName: Elasticsearch Cluster Name")
      System.err.println("            indexName: Index name for Elasticsearch, e.g. simulator")
      System.err.println("             typeName: Type name for Elasticsearch, e.g. simfile")
      System.err.println("         emitInterval: Number of seconds for spark streaming sampling")
      System.exit(1)
    }


    // spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master local[8] rtsink-jar-with-dependencies.jar a1:9092 simFile2 group3 a2:9200 elasticsearch simulator simfile2
    // java -cp Simulator-jar-with-dependencies.jar com.esri.simulator.Kafka 10.32.0.15:9476 simFile2 simFile_1000_10s.json 10 100

    // spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master local[8] rtsink-jar-with-dependencies.jar 10.32.0.15:9476 simFile2 group3 10.32.0.20:1025 mesos-ha simulator simfile2
    // java -cp Simulator-jar-with-dependencies.jar com.esri.simulator.Kafka a1:9092 simFile2 simFile_1000_10s.json 10000 100000

    // Set two config files (started with copy of templates)
    //   spark-defaults.conf --> Added line at end of file:  spark.executor.uri http://m1/apps/spark.tgz
    //   spark-env.sh --> Added line at end of file: export JAVA_HOME=/opt/mesosphere/active/java/usr/java
    // spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master mesos://m1:5050 rtsink-jar-with-dependencies.jar a1:9092 simFile2 group3 a2:9200 elasticsearch simulator simfile2

    val Array(brokers,topic,groupid,esNodes,esClusterName,esIndexName,esTypeName,emitInterval) = args

    /*
Tty settings like these
  val sConf = new SparkConf(true)
.setAppName(getClass.getSimpleName)
.set("spark.executor.memory", "4g")
.set("spark.cores.max", "16")
.set("spark.cleaner.ttl", "10000") // default is infinite
.set("spark.default.parallelism", "16") // default is num of cores
.set("spark.streaming.blockInterval", "50") // default is 200 ms
.set("spark.locality.wait", "250") // default is 3000 ms


 */

    /*
       curl hub2.marathon.mesos:13363/v1/connection | jq



       curl leader.mesos/marathon/v2/apps/hub2 | jq '.app.tasks'
       curl leader.mesos/marathon/v2/apps/hub2 | jq '.app.tasks[0].ports'

       curl leader.mesos/service/es/v1/tasks | jq
       curl leader.mesos/service/es/v1/cluster | jq




       java -cp Simulator-jar-with-dependencies.jar com.esri.simulator.Kafka 10.32.0.12:9650 simFile2 simFile_1000_10s.json 100 1000

       spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master local[8] rtsink-jar-with-dependencies.jar 10.32.0.12:9650 simFile2 group3 10.32.0.14:1025 mesos-ha simulator simfile2

      spark-submit --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.12:9650 simFile2 group3 10.32.0.14:1025 mesos-ha simulator simfile2

--executor-memory 1g --executor-cores 2 --conf spark.mesos.coarse=true --conf spark.cores.max=2


spark-submit --executor-memory 1g --executor-cores 2 --conf spark.mesos.coarse=true --conf spark.cores.max=2  --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.12:9650 simFile2 group3 10.32.0.14:1025 mesos-ha simulator simfile2

spark-submit --conf spark.executor.memory=4g --conf spark.cores.max=16 --conf spark.cleaner.ttl=10000 --conf spark.default.parallelism=16 --conf spark.streaming.blockInterval=50 --conf spark.locality.wait 250  --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.12:9650 simFile2 group3 10.32.0.14:1025 mesos-ha simulator simfile2

// Tried against locally running elastic (10.0.0.5); still input was around 11,000 e/s
spark-submit --conf spark.executor.memory=4g --conf spark.cores.max=16  --class com.esri.rtsink.scala.JsonKafkaElasticsearch --master mesos://leader.mesos:5050 rtsink-jar-with-dependencies.jar 10.32.0.12:9650 simFile2 group3 10.0.0.5:9200 elasticsearch simulator simfile2



      Rates around 13,000 e/s
curl 10.32.0.14:1025/sink/simfile2/_count

     */

    println(brokers)
    var kafkaBrokers = brokers
    val brokerSplit = brokers.split(":")
    if (brokerSplit.length == 1) {
      kafkaBrokers = new MarathonInfo().getBrokers(brokers)
    }
    println("brokers: " + kafkaBrokers)

    println(esNodes)
    var elasticNodes = esNodes
    var elasticClusterName = esClusterName
    val nodesSplit = esNodes.split(":")
    if (nodesSplit.length == 1) {
      val mi = new MarathonInfo()
      elasticNodes = mi.getElasticSearchHttpAddresses(esNodes);
      elasticClusterName = mi.getElasticSearchClusterName(esNodes);
    }
    println(elasticNodes)
    println(elasticClusterName)

    val sparkConf = new SparkConf().setAppName(appName)
    // Uncomment the following line to run from IDE
    //sparkConf.setMaster("local[8]")

    sparkConf.set("es.index.auto.create", "true").set("es.cluster.name",elasticClusterName).set("es.nodes", elasticNodes)

    case class simfile(rt: String, dtg: String, lon: Double, lat: Double)

    val ssc = new StreamingContext(sparkConf, Seconds(emitInterval.toInt))

    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> kafkaBrokers,"group.id" -> groupid)

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
