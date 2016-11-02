package io.datanerds.verteiler;

import org.apache.kafka.common.serialization.Deserializer;

public class ConsumerConfig<K, V> {

    public final String brokerBootstrap;
    public final String groupId;
    public final String topic;
    public final Deserializer<K> keyDeserializer;
    public final Deserializer<V> valueDeserializer;

    public ConsumerConfig(String brokerBootstrap, String groupId, String topic,
            Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.brokerBootstrap = brokerBootstrap;
        this.groupId = groupId;
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }
}
