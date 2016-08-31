/*
 Consume a Kafka topic and write lines to Elasticsearch.  Assumes the elements from Kafka are Json Strings suitable for loading into Elasticsearch.

 */
package com.esri.rtsink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.PartitionInfo;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Created by david on 8/20/2016.
 */
public class KafkaElasticsearch {
    String brokers;
    String topic;
    String group;
    String esnodes;
    String clusterName;
    String index;
    String typ;
    Integer esbulk;
    Integer webport;
    WebServer server;

    static final Pattern PATTERN = Pattern.compile("(([^\"][^,]*)|\"([^\"]*)\"),?");

    KafkaConsumer<String, String> consumer;

    Client client;

    public KafkaElasticsearch(String brokers, String topic, String group, String esnodes, String clusterName, String index, String typ, Integer esbulk, Integer webport) {
        this.brokers = brokers;
        this.topic = topic;
        this.group = group;
        this.esnodes = esnodes;
        this.clusterName = clusterName;
        this.index = index;
        this.typ = typ;
        this.esbulk = esbulk;
        this.webport = webport;



        try {

            Properties props = new Properties();
            props.put("bootstrap.servers",this.brokers);
            props.put("group.id", this.group);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", 1000);
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);

            Settings settings = Settings.settingsBuilder().put("cluster.name", this.clusterName).build();

            TransportClient tc = TransportClient.builder().settings(settings).build();

            String hosts[] = esnodes.split(",");
            for (String host : hosts) {
                String parts[] = host.split(":");
                InetAddress addr = InetAddress.getByName(parts[0]);
                Integer port = Integer.parseInt(parts[1]);

                tc.addTransportAddress(new InetSocketTransportAddress(addr, port));

                //System.out.println(addr.toString() + "," + port.toString());

            }

            this.client = (Client) tc;


            server = new WebServer(this.webport);

            //System.out.println(this.client);

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

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {  }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {  }
                })
                .setBulkActions(this.esbulk)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .build();


        //BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(10);
            // polls every 10ms
            Long ct = System.currentTimeMillis();

            if (cnt > 0 && ct - lr > 5000) {
                // Send the remaining items to elastic search
//                if (bulkRequest.numberOfActions() > 0) {
//                    BulkResponse bulkResponse = bulkRequest.get();
//                }
                bulkProcessor.close();
                bulkProcessor = BulkProcessor.builder(
                        client,
                        new BulkProcessor.Listener() {
                            @Override
                            public void beforeBulk(long executionId,
                                                   BulkRequest request) {  }

                            @Override
                            public void afterBulk(long executionId,
                                                  BulkRequest request,
                                                  BulkResponse response) {  }

                            @Override
                            public void afterBulk(long executionId,
                                                  BulkRequest request,
                                                  Throwable failure) {  }
                        })
                        .setBulkActions(this.esbulk)
                        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                        .setFlushInterval(TimeValue.timeValueSeconds(5))
                        .setConcurrentRequests(1)
                        .build();

                // Longer than 5 seconds reset and output stats
                long delta = lr - st;
                double rate = 1000.0 * (double) cnt / (double) delta;
                System.out.println(cnt + "," + rate);

                server.addRate(rate);
                server.setTm(System.currentTimeMillis());
                server.addCnt(cnt);
                cnt = 0L;
            }

            for (ConsumerRecord<String, String> record : records) {
                lr = System.currentTimeMillis();

                cnt += 1;
                if (cnt == 1) {
                    st = System.currentTimeMillis();
                }

                bulkProcessor.add(new IndexRequest(this.index, this.typ).source(record.value()));
//                bulkRequest.add(client.prepareIndex(this.index, typ).setSource(record.value()));
//
//                if (cnt % this.esbulk == 0) {
//                    BulkResponse bulkResponse = bulkRequest.get();
//                    bulkRequest = client.prepareBulk();
//
//                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        // Example Arguments: a1:9092 simFile2 group3 a2:9300 elasticsearch sink simfile2 1000 16001
        // Example Arguments: hub2 simFile2 group3 elasticsearch - sink simfile2 1000

        // You can specify the Marthon name of Elasticsearch and the app looks up ips of tasks; then - for clusterName to look that up too


        int numArgs = args.length;

        if (numArgs != 9) {
            System.err.print("Usage: KafkaElasticsearch <broker-list> <topic> <group-id> <es-transport-nodes> <clusterName> <indx> <typ> <es-bulk> <web-port> \n");
        } else {


            String brokers = args[0];
            String topic = args[1];
            String groupId = args[2];
            String esnodes = args[3];
            String clusterName = args[4];
            String index = args[5];
            String typ = args[6];
            Integer bulk = Integer.parseInt(args[7]);
            Integer webport = Integer.parseInt(args[8]);

            String brokerSplit[] = brokers.split(":");
            if (brokerSplit.length == 1) {
                // Try hub name. Name cannot have a ':' and brokers must have it.
                brokers = new MarathonInfo().getBrokers(brokers);
            }   // Otherwise assume it's brokers

            String esnodesSplit[] = esnodes.split(":");
            if (esnodesSplit.length == 1) {
                MarathonInfo mi = new MarathonInfo();
                if (clusterName.equalsIgnoreCase("-")) {
                    clusterName = mi.getElasticSearchClusterName(esnodes);
                }

                // Try hub name. Name cannot have a ':' and brokers must have it.
                esnodes = mi.getElasticSearchTransportAddresses(esnodes);



            }   // Otherwise assume it's brokers

            KafkaElasticsearch t = null;


            System.out.println(brokers);
            System.out.println(topic);
            System.out.println(groupId);
            System.out.println(esnodes);
            System.out.println(clusterName);
            System.out.println(index);
            System.out.println(typ);
            System.out.println(bulk);
            System.out.println(webport);


            t = new KafkaElasticsearch(brokers,topic,groupId,esnodes,clusterName,index,typ,bulk,webport);
            t.read();

        }
    }
}
