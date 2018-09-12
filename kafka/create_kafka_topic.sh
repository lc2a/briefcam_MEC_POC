#!/bin/sh
/opt/kafka/bin/kafka-topics.sh --create --zookeeper mec-poc:2181 --replication-factor 1 --partitions 10 --topic topic_key
