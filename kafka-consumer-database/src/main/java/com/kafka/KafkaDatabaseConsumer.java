package com.kafka;

import com.kafka.entity.WikimediaData;
import com.kafka.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    // Implement Kafka consumer logic here to consume messages from the producer

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @Autowired
    private WikimediaDataRepository wikimediaDataRepository;



    @KafkaListener(topics ="${spring.kafka.topic.name}",groupId ="${spring.kafka.consumer.group-id}")
    public void consume(String eventMessage){


        LOGGER.info("event message received : " + eventMessage);

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikimediaEventData(eventMessage);

        wikimediaDataRepository.save(wikimediaData);


    }
}
