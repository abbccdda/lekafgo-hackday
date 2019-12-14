#!/usr/bin/env bash

echo "===>Start Zookeeper:"
./kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties > zookeeper.log 2>&1 &
echo "Zookeeper PID: $!"
sleep 10
echo "===>Start Kafka:"
./kafka/bin/kafka-server-start.sh kafka/config/server.properties > kafka.log 2>&1 &
echo "Kafka PID: $!"
