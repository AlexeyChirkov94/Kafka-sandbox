package ru.chirkov.kafka.producer.service;

import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.chirkov.kafka.producer.model.KafkaMessage;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class MessageSender {

    private final KafkaTemplate<String, KafkaMessage> template;
    private final String topicName;
    private final ScheduledExecutorService executor;
    private final AtomicLong nextValue;
    private Future<?> future;

    public MessageSender(KafkaTemplate<String, KafkaMessage> template, @Value("${kafka.topic.name}") String topicName) {
        this.template = template;
        this.topicName = topicName;
        this.executor = Executors.newScheduledThreadPool(1);
        this.nextValue = new AtomicLong(0);
    }

    public void send(KafkaMessage message){
        if (message == null) message = generateKafkaMessage(null);
        if (message.getId() == null) message.setId(nextValue.getAndIncrement());

        try{
            KafkaMessage finalMessage = message;
            template.send(topicName, message)
                    .whenComplete(
                            (result, ex) -> {
                                if (ex == null) {
                                    log.info("message id:{} was sent, offset:{}", finalMessage.getId(), result.getRecordMetadata().offset());
                                } else {
                                    log.error("message id:{} was not sent", finalMessage.getId(), ex);
                                }
                            }
                    );
        } catch (Exception exception){
            log.error("send error, value:{}", message, exception);
        }
    }

    public void sendManyMessages(@Nullable Long idOfFirstMessage, Integer countOfMessage) {
        if (idOfFirstMessage == null) idOfFirstMessage = nextValue.getAndIncrement();
        for (int i = 0; i < countOfMessage; i++) {
            KafkaMessage item = generateKafkaMessage(idOfFirstMessage + i);
            send(item);
        }
    }

    public void startGenerateMessagePeriodically(@Nullable Long idOfFirstMessage, Long period, TimeUnit timeUnit){
        if (idOfFirstMessage != null) nextValue.set(idOfFirstMessage);
        future = executor.scheduleAtFixedRate(() -> send(generateKafkaMessage(null)), 0, period, timeUnit);
    }

    public void stopGenerateMessagePeriodically(){
        if (future != null) future.cancel(true);
    }

    private KafkaMessage generateKafkaMessage(@Nullable Long id) {
        if (id != null) return new KafkaMessage(id, "value = " + id);
        else return generateKafkaMessage(nextValue.getAndIncrement());
    }

    @PreDestroy
    private void preDestroy(){
        executor.shutdown();
    }

}
