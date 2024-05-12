package io.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Program to observe same key is going in to same partition.
 *
 */
public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main( String[] args ) {
        log.info( "I am a Producer." );

        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "second_topic";
                String key = "id_" + i;
                String value = "Hello_" + i;
                //create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Key: " + key + "| Partition: " + metadata.partition());
                        } else {
                            log.error("Exception while producing: ", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //tell the producer to send all data and block until done --- synchronous
        producer.flush();

        producer.close();
    }
}
