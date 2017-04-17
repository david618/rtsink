# Retired

This project as merged with rsource and development will continue under name [rt](https://github.com/david618/rt/).



# rtsink

Test sinks for real time DCOS/Mesos/Marathon apps.

## Installation

Java project. Orginally created in NetBeans 8.1. Also imported and built in IntelliJ.

Tested with maven 3.3.9
$ mvn install 

The target folder will contain:
- lib folder: all of the jar depdencies
- rtsink.jar: small executable jar (w/o dependencies)
- rtsink-jar-with-dependencies.jar: larget executable jar with dependencies.

## Usage

You can run them from the command line:

$ java -cp rtsink.jar com.esri.rtsink.KafkaCnt

Usage: rtsink <broker-list-or-hub-name> <topic> <group-id> <web-port>

$ java -cp rtsink.jar com.esri.rtsink.KafkaCnt rth simFile group1 14002

KafkaCnt connects to rth broker (deployed in DCOS) and consumes the simFile topic as group1.  It will read and count any records that show up on the topic. After a burst of inputs it prints the count and rate.  The count and rate are also available on the web-port. (http://localhost:14002/count).

This app can also be ran in Mesos/Marathon.  http://davidssysadminnotes.blogspot.com/2016/08/performance-testing-kafka-on-dcos.html 

Additional classes are in development (e.g. KafkaStdout, KafkaGeoFenceStdout)

## License

http://www.apache.org/licenses/LICENSE-2.0 
