package org.acme;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class KafkaPoc {

    private static final Logger LOG = Logger.getLogger(KafkaPoc.class);

    private static final String TOPIC = "hello";
    private static final int AMOUNT_EVENTS = 100;
    private static final int KAFKA_POLL_DURATION_SEC = 10;
    private static final int TPS_LIMIT = 3;
    private static final int INITIAL_EVENTS_AMOUNT = 0;

    @Inject
    @Named("kafka-consumer")
    private KafkaConsumer<String, String> kafkaConsumer;

    @Inject
    @Named("kafka-producer")
    private KafkaProducer<String, String> kafkaProducer;

    public void initialize(@Observes StartupEvent ev) throws ExecutionException, InterruptedException, TimeoutException {

        // generate events
        for(int i = 0; i < AMOUNT_EVENTS; i++) {
            kafkaProducer.send(new ProducerRecord<>(TOPIC, UUID.randomUUID().toString(), UUID.randomUUID().toString())).get(5, TimeUnit.SECONDS);
        }

        // consume 3 events per second
        AtomicInteger eventsCount = new AtomicInteger();
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        new Thread(() -> {
            try {
                while (true) {
                    final ConsumerRecords<String, String> events = kafkaConsumer.poll(Duration.ofSeconds(KAFKA_POLL_DURATION_SEC));
                    for(ConsumerRecord<String, String> record : events) {
                        if(eventsCount.get() >= TPS_LIMIT) {
                            LOG.info("--- Company TPS limit reached ---");
                            Collection<TopicPartition> topicPartitions =  Collections.singleton(new TopicPartition(TOPIC, record.partition()));
                            kafkaConsumer.pause(topicPartitions); // https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#pause(java.util.Collection)

                            // hold consumer thread
                            Thread.sleep(Duration.ofSeconds(TPS_LIMIT).toMillis());
                            eventsCount.set(INITIAL_EVENTS_AMOUNT);
                            kafkaConsumer.resume(topicPartitions); // https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
                        } else {
                            LOG.info(String.format("Polled Record:(%s, %s, %d, %d)",
                                    record.key(), record.value(),
                                    record.partition(), record.offset()));

                            kafkaConsumer.commitSync();
                            eventsCount.getAndIncrement();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
            } finally {
                kafkaConsumer.close();
            }
        }).start();
    }
}
