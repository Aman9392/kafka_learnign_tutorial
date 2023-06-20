package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        String groupID = "my-kafka-application";
        String topic = "demo_java";

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1cp2mb4dP1UITDEokxNSN7\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxY3AybWI0ZFAxVUlUREVva3hOU043Iiwib3JnYW5pemF0aW9uSWQiOjczODk3LCJ1c2VySWQiOjg1OTQ3LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwNjNmYWRlOC1jNzM4LTQwM2MtOWQ0Mi1mNzg3MzY3NGIyYjMifX0.ufufEEdOuQQwojb8wvUDGIswVOLnLTNhITEkypBFcfc\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a  reference to the main thread
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a ShutDown,let's by calling consumer .wakeup()..");
                consumer.wakeup();
                //join the main thread to allow the execution of a code in  main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
            // poll for data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }
        }catch(WakeupException e){
            log.info("Consumer Starting to shut down");
        }catch (Exception e){
            log.info("Unexpected exception in the consumer" ,e);
        }finally{
            consumer.close();// close the consumer , this will also commit offset
            log.info("The consumer is know gracefully shut down");
        }
    }
}