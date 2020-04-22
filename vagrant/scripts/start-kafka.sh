#!/bin/bash

source "/vagrant/scripts/common.sh"

function startKafka {
    echo "starting Kafka"
    sed -i -e 's/clientPort=2181/clientPort=2189/g' /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/config/zookeeper.properties
    sed -i -e 's/zookeeper.connect=localhost:2181/zookeeper.connect=localhost:2189/g' /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties
    /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/bin/zookeeper-server-start.sh /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/config/zookeeper.properties &
    /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/bin/kafka-server-start.sh /usr/local/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties &
}

startKafka
