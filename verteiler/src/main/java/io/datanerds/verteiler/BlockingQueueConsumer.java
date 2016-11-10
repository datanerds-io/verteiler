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
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;

/**
 * This class leverages the kafka-clients consumer implementation to distribute messages from assigned partitions to
 * {@link java.util.concurrent.BlockingQueue}s. Each assigned partition will relay its messages to its own queue.
 * In addition, each queue has a consuming process/thread. Once a message has been "processed" successfully its offset
 * will be marked to be committed.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class BlockingQueueConsumer<K, V> implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumer.class);

    private final Properties kafkaConfig;
    private final String topic;
    private final int queueSize;
    private final java.util.function.Consumer<V> action;

    private final Map<Integer, Processor<K, V>> processors = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final Consumer<K, V> consumer;

    private final Object lock = new Object();
    private volatile ConsumerRecordRelay<K, V> relay;

    /**
     * This constructor immediately connects to the kafka broker and creates a {@link Processor} for each assigned
     * partition.
     *
     * @param topic       The topic to subscribe to
     * @param kafkaConfig Kafka client configuration
     * @param queueSize   Size of the {@link java.util.concurrent.BlockingQueue}s for each {@link Processor}
     * @param action      {@link java.util.function.Consumer} of the transported message
     * @throws IllegalArgumentException if problems are found with the config
     */
    public BlockingQueueConsumer(String topic, Properties kafkaConfig, int queueSize,
            java.util.function.Consumer<V> action) {
        this.topic = topic;

        KafkaConfigValidator.validate(kafkaConfig);
        this.kafkaConfig = kafkaConfig;

        this.action = action;
        this.queueSize = queueSize;
        this.pool = Executors.newCachedThreadPool();
        this.consumer = createKafkaConsumer();

        consumer.subscribe(Arrays.asList(topic), this);
        Set<TopicPartition> partitions = consumer.assignment();
        partitions.forEach(this::createProcessor);
    }

    /**
     * Start consuming/ relaying messages to the processors.
     *
     * @throws IllegalStateException in case the consumer has been started before
     */
    public void start() {
        synchronized (lock) {
            if (relay != null) {
                throw new IllegalStateException("Consumer already started");
            }
            relay = new ConsumerRecordRelay<>(consumer, this);
            new Thread(relay).start();
        }
    }

    /**
     * Stops all background activities: kafka message consumption, message relay and processing.
     *
     * @throws IllegalArgumentException in case the consumer has not been started
     */
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
        if (!topic.equals(message.topic())) {
            throw new ConsumerException(String.format("Message from unexpected topic: '%s'", message.topic()));
        }
        Processor<K, V> processor = processors.get(message.partition());
        processor.queue(message);
    }

    private void createProcessor(TopicPartition partition) {
        Processor<K, V> processor = new Processor<>(partition, relay, action, queueSize);
        pool.execute(processor);
        processors.put(partition.partition(), processor);
    }

    private Consumer<K, V> createKafkaConsumer() {
        addClientIdIfNotPresent(kafkaConfig);
        addOffsetResetConfigIfNotPresent(kafkaConfig);
        return new KafkaConsumer<>(kafkaConfig);
    }

    private void addClientIdIfNotPresent(Properties props) {
        if (!props.contains(CLIENT_ID_CONFIG)) {
            props.put(CLIENT_ID_CONFIG, getClientId());
        }
    }

    private void addOffsetResetConfigIfNotPresent(Properties props) {
        if (!props.contains(AUTO_OFFSET_RESET_CONFIG)) {
            props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
    }

    private String getClientId() {
        try {
            return String.format("%s-%s", InetAddress.getLocalHost().getHostName(), topic);
        } catch (UnknownHostException ex) {
            throw new ConsumerException("Could not retrieve client identifier", ex);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(this::createProcessor);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            Processor<K, V> processor = processors.get(partition.partition());
            processor.stop();
            processors.remove(partition.partition());
            relay.removePartitionFromOffset(partition);
        });
    }
}
