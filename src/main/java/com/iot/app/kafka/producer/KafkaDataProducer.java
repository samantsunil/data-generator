package com.iot.app.kafka.producer;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.iot.app.kafka.util.PropertyFileReader;
import com.iot.app.kafka.vo.StreamData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * IoT data event producer class which uses Kafka producer for events.
 */
public class KafkaDataProducer {

    private static final Logger logger = Logger.getLogger(KafkaDataProducer.class);

    public static void main(String[] args) throws Exception {
        //read config file
        Properties prop = PropertyFileReader.readPropertyFile();
        String zookeeper = prop.getProperty("com.iot.app.kafka.zookeeper");
        String brokerList = prop.getProperty("com.iot.app.kafka.brokerlist");
        String topic = prop.getProperty("com.iot.app.kafka.topic");
        int noOfVehicles = Integer.parseInt(prop.getProperty("com.iot.app.kafka.noofvehicles"));
        logger.info("Using Zookeeper=" + zookeeper + " , Broker-list=" + brokerList + " , no of vehicles= " + noOfVehicles + " and topic= " + topic + "\r\n");

        // set producer properties for kafka
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("bootstrap.servers", brokerList);
        properties.put("acks", "0"); //never waits for an ack from the broker, to achieve low latency.
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.iot.app.kafka.util.IoTDataEncoder");
        //generate event
        Producer<String, StreamData> producer = new KafkaProducer<>(properties);
        KafkaDataProducer iotProducer = new KafkaDataProducer();
        iotProducer.generateIoTEvent(producer, topic, noOfVehicles);
    }

    private void generateIoTEvent(Producer<String, StreamData> producer, String topic, int noOfVehicles) throws InterruptedException {
        List<String> routeList = Arrays.asList(new String[]{"Route-M1", "Route-M2", "Route-M3"});
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Truck", "Car", "Bus", "Taxi"});
        Random rand = new Random();
        logger.info("Sending events");
        final long NANOSEC_PER_SEC = 1000l * 1000 * 1000;
        int timeIntervalToRun = 30;
        long startTime = System.nanoTime();
        // generate event in loop
        while ((System.nanoTime() - startTime) < timeIntervalToRun * 60 * NANOSEC_PER_SEC) {
            for (int i = 0; i < noOfVehicles; i++) {
                String vehicleId = UUID.randomUUID().toString();
                String vehicleType = vehicleTypeList.get(rand.nextInt(4));
                String routeId = routeList.get(rand.nextInt(3));
                Date timestamp = new Date();
                double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
                double fuelLevel = rand.nextInt(40 - 10) + 10;

                String coords = getCoordinates(routeId);
                String latitude = coords.substring(0, coords.indexOf(","));
                String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
                StreamData event = new StreamData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed, fuelLevel);
                ProducerRecord<String, StreamData> data = new ProducerRecord<>(topic, event);
                producer.send(data);
            }
            Thread.sleep(1000);
        }
    }

    //Method to generate random latitude and longitude for routes
    private String getCoordinates(String routeId) {

        Random rand = new Random();
        int latPrefix = 0;
        int longPrefix = -0;
        switch (routeId) {
            case "Route-M1":
                latPrefix = 33;
                longPrefix = -96;
                break;
            case "Route-M2":
                latPrefix = 34;
                longPrefix = -97;
                break;
            case "Route-M3":
                latPrefix = 35;
                longPrefix = -98;
                break;
            default:
                break;
        }
        Float lati = latPrefix + rand.nextFloat();
        Float longi = longPrefix + rand.nextFloat();
        return lati + "," + longi;
    }
}
