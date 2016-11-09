package io.datanerds.verteiler;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

public class ConsumerConfig<K, V> {

    public final String topic;
    public final Properties consumerProperties;
    public final Deserializer<K> keyDeserializer;
    public final Deserializer<V> valueDeserializer;

    public ConsumerConfig(String topic,
                          Properties consumerProperties,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
        this.topic = topic;
        this.consumerProperties = consumerProperties;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }
}
