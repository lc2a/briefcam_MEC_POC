#!/bin/sh
/opt/kafka/bin/kafka-topics.sh --create --zookeeper $KAFKA_ZOOKEEPER_CONNECT --replication-factor 2 --partitions 10 --topic $topic_key
