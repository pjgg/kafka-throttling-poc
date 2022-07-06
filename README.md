# kafka-throttling-poc

Kafka DocRef: https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

Quarkus DocRef: https://quarkus.io/guides/kafka

Start Kafka: go to resource folder and run `docker-compose -f docker-compose-strimzi.yaml up`

Start Demo: go to app main folder and run `mvn quarkus:dev`

This DEMO will generate 100 of events and then will consume these events with a RATE LIMIT of 3 events per second.