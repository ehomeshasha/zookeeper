#!/usr/bin/env bash

/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.51.x86_64/bin/java \
-Dzookeeper.log.dir=. -Dzookeeper.root.logger=INFO,CONSOLE \
-cp /home/hadoop-user/workspace/zookeeper/bin/../build/classes:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/slf4j-log4j12-1.7.5.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/slf4j-api-1.7.5.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/netty-3.7.0.Final.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/log4j-1.2.16.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/jline-2.11.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/javacc.jar:/home/hadoop-user/workspace/zookeeper/bin/../build/lib/commons-cli-1.2.jar:/home/hadoop-user/workspace/zookeeper/bin/../lib/*.jar:/home/hadoop-user/workspace/zookeeper/bin/../zookeeper-*.jar:/home/hadoop-user/workspace/zookeeper/bin/../src/java/lib/ivy-2.2.0.jar:/home/hadoop-user/workspace/zookeeper/bin/../src/java/lib/ant-eclipse-1.0-jvm1.2.jar:/home/hadoop-user/workspace/zookeeper/bin/../conf:/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.51.x86_64/lib/dt.jar:/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.51.x86_64/lib/tools.jar:/home/hadoop-user/lib/mahout-distribution-0.9/lib:/home/hadoop-user/lib/hadoop-1.2.1/lib \
-Xmx1000m \
-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false \
 org.apache.zookeeper.server.quorum.QuorumPeerMain \
 /home/hadoop-user/workspace/zookeeper/bin/../conf/zoo.cfg
