package io.datanerds.verteiler.it_test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleTestProducer {
    private final String producerName;
    private final String topic;
    private final KafkaProducer<String, String> producer;

    public SimpleTestProducer(String producerName, String topic, String broker) {
        this.producerName = producerName;
        this.topic = topic;
        this.producer = create(broker);
    }

    private KafkaProducer<String, String> create(String broker) {
        Properties config = new Properties();
        config.put(ProducerConfig.CLIENT_ID_CONFIG, this.producerName);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    /**
     * No order guarantees here since there is no key we partition by.
     *
     * @param message
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public RecordMetadata send(String message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        return this.producer.send(record).get();
    }

    public void close() {
        producer.close();
    }
}
