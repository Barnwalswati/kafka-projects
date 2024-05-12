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
 * Hello world!
 *
 */
public class ProducerDemoWithCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

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

        for (int i=0; i<10; i++) {
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

        //tell the producer to send all data and block until done --- synchronous
        producer.flush();

        producer.close();

        /*If you observe the behaviour of the kafka producer, you will see in output like every message is producing in the
        same partition, this is called StickyPartitioner.In this case producer is batching messages into a batch(default)
        When key=null, the producer has a default partitioner that varies:

        Round Robin: for Kafka 2.3 and below

        Sticky Partitioner: for Kafka 2.4 and above

        Sticky Partitioner improves the performance of the producer especially with high throughput.*/
    }
}
