package org.acme;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class KafkaPoc {

    private static final Logger LOG = Logger.getLogger(KafkaPoc.class);

    private static final String TOPIC = "hello";
    private static final int KAFKA_POLL_DURATION_SEC = 10;
    private static final int TPS_LIMIT = 3;
    private static final int INITIAL_EVENTS_AMOUNT = 0;

    @Inject
    @Named("kafka-consumer")
    private KafkaConsumer<String, String> kafkaConsumer;

    @Inject
    @Channel("producer-source")
    @OnOverflow(value = OnOverflow.Strategy.DROP)
    Emitter<String> emitter;

    @Channel("channel-example")
    Multi<String> events;

    public void initialize(@Observes StartupEvent ev) {

        // generate events
        Multi.createFrom().ticks().every(Duration.ofMillis(100)).subscribe().with(i ->{
            emitter.send("" + i).whenComplete(handlerEmitterResponse(KafkaPoc.class.getName()));
        });

        // consume 3 events per second (sync impl example)
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

        // consume 3 events per second (async impl example)
         events
                .ifNoItem().after(Duration.ofMillis(20000)).fail()
                .onFailure().retry().withBackOff(Duration.ofSeconds(2), Duration.ofSeconds(10)).indefinitely()
                .group().intoLists().of(3, Duration.ofSeconds(1))
                 .call(() -> Uni.createFrom().nullItem().onItem().delayIt().by(Duration.ofMillis(3000)))
                .onItem().transformToMultiAndConcatenate(l -> Multi.createFrom().iterable(l))
                .subscribe().with(LOG::info);
    }

    @NotNull
    private BiConsumer<Void, Throwable> handlerEmitterResponse(final String owner) {
        return (success, failure) -> {
            if (failure != null) {
                LOG.error(String.format("D'oh! %s", failure.getMessage()));
            }
        };
    }
}
