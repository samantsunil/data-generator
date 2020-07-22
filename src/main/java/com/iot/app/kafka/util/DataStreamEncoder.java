package com.iot.app.kafka.util;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iot.app.kafka.vo.StreamData;

import org.apache.kafka.common.serialization.Serializer;

public class DataStreamEncoder implements Serializer<StreamData> {
	
	private static final Logger logger = Logger.getLogger(DataStreamEncoder.class);
        private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, StreamData message)
    {
        if (message == null) {
            return null;
        }

        try {

                       String msg = objectMapper.writeValueAsString(message);
			//logger.info(msg);
			return msg.getBytes();
            
        } catch (JsonProcessingException e) {
			logger.error("Error in Serialization \r\n", e);
		}
        return null;
    }

    @Override
    public void close()
    {

    }	
}
