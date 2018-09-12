#!/bin/sh
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --partitions 10 --topic $topic_key
