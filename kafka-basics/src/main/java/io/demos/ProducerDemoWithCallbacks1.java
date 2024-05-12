package io.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Program to see multiple batches with StickyPartitioner
 *
 */
public class ProducerDemoWithCallbacks1 {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks1.class.getSimpleName());

    public static void main( String[] args ) {
        log.info( "I am a Producer." );

        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //just to see different batch (Bcz default = 16KB)
//        properties.setProperty("batch.size", "400");
//
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<10; j++) {
            for (int i = 0; i < 30; i++) {
                //create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("second_topic", "hello-world " + i);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Exception while producing: ", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //tell the producer to send all data and block until done --- synchronous
        producer.flush();

        producer.close();
    }
}
