package com.gurpreet.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class POCProducer {

    final static String bootStrapServer = "127.0.0.1";
    //create producer properties
    protected Properties getKafkaProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    protected KafkaProducer<String,String> getProducer() {
        //create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getKafkaProperties());
        return producer;
    }

    public void producer_send(String message){
        ProducerRecord<String,String> record = new ProducerRecord<>("topic_report_event",message);
        //send data async
        KafkaProducer<String, String> producer = getProducer();
        producer.send(record);
        //flush
        producer.flush();
        producer.close();
    }


}
