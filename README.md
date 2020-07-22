# Data Stream Generator for Kafka Service
Data generator is a maven application for generating data streams as IoT data events using Apache Kafka producer API. This project is build using following tools and technologies.

- JDK - 1.8
- Maven - 3.3.9
- Kafka - 2.12-2.4.1

You can build and run this application using below commands. Please check resources/iot-kafka.properties for configuration details and place the in the target directory to be used in CROODaP application or modify accordingly in propertyfilereader class.

```sh
sudo mvn package
sudo java -jar iot-kafka-producer-1.0.0.jar"

```

The application requires configuration for kafka brokers addresses and zookeeper server address before sending data to the kafka cluster.
