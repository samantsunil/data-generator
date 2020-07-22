#!/bin/bash
cd /opt/kafka/bin/
sudo bash kafka-topics.sh --create --zookeeper public-iporDNS:2181 --replication-factor 1 --partitions 6 --topic vehicle-data-stream
cd ~
