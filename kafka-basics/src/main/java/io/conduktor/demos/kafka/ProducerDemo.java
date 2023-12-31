package io.conduktor.demos.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");
        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers" , "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol" , "SASL_SSL");
        properties.setProperty("sasl.jaas.config"  , "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1cp2mb4dP1UITDEokxNSN7\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxY3AybWI0ZFAxVUlUREVva3hOU043Iiwib3JnYW5pemF0aW9uSWQiOjczODk3LCJ1c2VySWQiOjg1OTQ3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwNjNmYWRlOC1jNzM4LTQwM2MtOWQ0Mi1mNzg3MzY3NGIyYjMifX0.ufufEEdOuQQwojb8wvUDGIswVOLnLTNhITEkypBFcfc\";");
        properties.setProperty("sasl.mechanism" , "PLAIN");

        properties.setProperty("key.serializer",StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        // send data - asynchronous
        producer.send(producerRecord);

        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }
}