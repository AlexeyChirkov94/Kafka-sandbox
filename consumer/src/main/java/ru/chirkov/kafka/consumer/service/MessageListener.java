package ru.chirkov.kafka.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import ru.chirkov.kafka.consumer.model.KafkaMessage;

import java.util.List;

@Slf4j
@Service
public class MessageListener {

    @KafkaListener(topics = "${kafka.topic.name}", containerFactory = "listenerContainerFactory", groupId = "${spring.kafka.consumer.group-id")
    public void listen(@Payload List<KafkaMessage> messages) {
        log.info("values, values.size:{}", messages.size());
        for (var message : messages) log.info("message:{}", message);
    }

}
