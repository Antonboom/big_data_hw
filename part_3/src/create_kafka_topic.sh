#!/usr/bin/env bash

TOPIC_NAME=$1
kafka-topics --create --zookeeper localhost:2181 --topic ${TOPIC_NAME} --partition 1 --replication-factor 1

