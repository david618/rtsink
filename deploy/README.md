# Deploy instructions

The kafka-cnt.json file is an example Marathon configuration for this application.

## id
The id must be lowercase

## cmd
The $MESOS_SANDBOX is where the app can access contents it retrieves from "uris" below.

Runs a Java comman to execute the class com.esri.rtsink.KafkaCnt

Parameters: 172.16.0.4:9092 simFile group1 $PORT0 true
- 172.16.0.4:9092: is the ip:port of Kafka; this parameter could also be the name of the DCOS Kafka app in Marathon (e.g. hub2)
- simFile: topic name
- group1: Group ID for Kafka
- $PORT0: Use Marathon assigned port for health check; the app will also make results accessible on this port
- true: Optional parameter for testing latency.  Latency assumes CSV inputs and the last field is epoch time in milliseconds.


## cpus, mem, and disk
You may need to increase these. I originally started with low values; however, during testing the application failed. In Marathon I found an error about insufficent memory. 

## constraints
I have configured hostname:UNIQUE. This is not necessary for this sink; however, for testing I used this to keep multiple instances from running on same agent.

## healthChecks
Looks for response on $PORT0. No resonse and Marathon will restart the application.

## uris
You'll need to put the JRE, libs, and rtsink jar on a web server that is accessible from the Marathon agent nodes.

"uris": [
    "http://172.16.0.5/apps/jre-8u91-linux-x64.tar.gz",
    "http://172.16.0.5/apps/rtlib.tgz",
    "http://172.16.0.5/apps/rtsink.jar"
  ]
