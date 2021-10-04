package com.gurpreet.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ProducerWithCallback extends POCProducer{

    final static Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);


    @Override
    public void producer_send(String message){

        KafkaProducer<String,String> producer = getProducer();
        ProducerRecord<String,String> record = new ProducerRecord<>("topic_log_event",message);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e!=null){
                    log.info("Topic : "+recordMetadata.topic()+"\n Offset : "+recordMetadata.offset()
                            +" Partition : "+recordMetadata.partition()+" Timestamp : "+recordMetadata.timestamp());
                    recordMetadata.offset();
                }else{
                    log.error("Exception caught : ",e);
                }
            }
        });
    }
}
