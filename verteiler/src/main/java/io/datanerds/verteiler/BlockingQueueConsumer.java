package io.datanerds.verteiler;

import com.google.common.util.concurrent.MoreExecutors;
import io.datanerds.verteiler.exception.ConsumerException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class BlockingQueueConsumer<K, V> implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumer.class);

    private final ConsumerConfig<K, V> config;
    private final int queueSize;
    private final java.util.function.Consumer<V> action;

    private final Map<Integer, Processor<K, V>> processors = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final Consumer<K, V> consumer;

    private final Object lock = new Object();
    private volatile ConsumerRecordRelay<K, V> relay;

    public BlockingQueueConsumer(ConsumerConfig<K, V> config, int queueSize, java.util.function.Consumer<V> action) {
        this.config = config;
        this.action = action;
        this.queueSize = queueSize;
        this.pool = Executors.newCachedThreadPool();
        this.consumer = createKafkaConsumer();

        consumer.subscribe(Arrays.asList(config.topic), this);
        Set<TopicPartition> partitions = consumer.assignment();
        partitions.forEach(this::createProcessor);
    }

    public void start() {
        synchronized (lock) {
            if (relay != null) {
                throw new IllegalStateException("Consumer already started");
            }
            relay = new ConsumerRecordRelay<>(this.consumer, this);
            new Thread(this.relay).start();
        }
    }

    public void stop() {
        synchronized (lock) {
            if (relay == null) {
                throw new IllegalStateException("Consumer not started, nothing to stop");
            }
            relay.stop();

            if (!MoreExecutors.shutdownAndAwaitTermination(pool, 10, SECONDS)) {
                logger.error("Pool was not terminated properly.");
            }
        }
    }

    void relay(ConsumerRecord<K, V> message) throws InterruptedException {
        if (!config.topic.equals(message.topic())) {
            throw new ConsumerException(String.format("Message from unexpected topic: '%s'", message.topic()));
        }
        Processor<K, V> processor = processors.get(message.partition());
        processor.queue(message);
    }

    private void createProcessor(TopicPartition partition) {
        Processor<K, V> processor = new Processor<>(relay, action, queueSize);
        pool.execute(processor);
        processors.put(partition.partition(), processor);
    }

    private Consumer<K, V> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, config.brokerBootstrap);
        props.put(GROUP_ID_CONFIG, config.groupId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(CLIENT_ID_CONFIG, getClientId());
        return new KafkaConsumer<>(props, config.keyDeserializer, config.valueDeserializer);
    }

    private String getClientId() {
        try {
            return String.format("%s-%s", InetAddress.getLocalHost().getHostName(), config.topic);
        } catch (UnknownHostException ex) {
            throw new ConsumerException("Could not retrieve client identifier", ex);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach((this::createProcessor));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach((partition -> {
            Processor<K, V> processor = processors.get(partition.partition());
            processor.stop();
            processors.remove(partition.partition());
        }));
    }
}
