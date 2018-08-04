package io.datanerds.verteiler;

import com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ConsumerRecordRelay<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRecordRelay.class);
    private static final long POLLING_TIMEOUT_MS = 5000L;

    private volatile boolean stopped = false;
    private volatile boolean updateOffsets;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
    private final BlockingQueueConsumer<K, V> blockingQueueConsumer;
    private final Consumer<K, V> consumer;

    public ConsumerRecordRelay(Consumer<K, V> consumer, BlockingQueueConsumer<K, V> blockingQueueConsumer) {
        this.blockingQueueConsumer = blockingQueueConsumer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (!stopped) {
            ConsumerRecords<K, V> records = consumer.poll(POLLING_TIMEOUT_MS);
            for (ConsumerRecord<K, V> record : records) {
                try {
                    blockingQueueConsumer.relay(record);
                } catch (InterruptedException ignored) {
                    logger.info("Interrupted during relay");
                } catch (Exception ex) {
                    logger.error("Error while relaying messages from kafka to queue: {}", ex.getMessage(), ex);
                    blockingQueueConsumer.stop();
                    break;
                }
            }
            commitOffsets();
        }
        this.consumer.close();
        logger.info("Kafka message relay stopped");
    }

    public void setOffset(ConsumerRecord<K, V> record) {
        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        updateOffsets = true;
    }

    public void removePartitionFromOffset(TopicPartition topicPartition) {
        offsets.remove(topicPartition);
    }

    private void commitOffsets() {
        if (updateOffsets) {
            consumer.commitAsync(offsets, this::callback);
            updateOffsets = false;
        }
    }

    void stop() {
        logger.info("Stopping Kafka message relay");
        stopped = true;
    }

    private void callback(Map<TopicPartition, OffsetAndMetadata> offset, Exception ex) {
        if (ex != null) {
            Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
            logger.error("Error during offset commit: '{}'", mapJoiner.join(offset), ex);
        }
    }
}
