package io.datanerds.verteiler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class Processor<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    private volatile boolean stopped = false;

    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final ConsumerRecordRelay<K, V> relay;
    private final java.util.function.Consumer<V> action;
    private final TopicPartition topicPartition;

    Processor(TopicPartition topicPartition, ConsumerRecordRelay<K, V> relay, java.util.function.Consumer<V> action,
            int queueSize) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.relay = relay;
        this.action = action;
        this.topicPartition = topicPartition;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(topicPartition.toString());
        logger.info("Processor for {} started", topicPartition);
        try {
            while (!stopped) {
                ConsumerRecord<K, V> record = queue.take();
                action.accept(record.value());
                relay.setOffset(record);
            }
        } catch (InterruptedException ignored) {
            logger.debug("Processor for {} interrupted while waiting for messages", topicPartition);
        } catch (Exception ex) {
            logger.error("Exception during processing {}. Stopping!", topicPartition, ex);
        }
        stopped = true;
        queue.clear();
        logger.info("Processor for {} stopped", topicPartition);
    }

    public void stop() {
        stopped = true;
    }

    public void queue(ConsumerRecord<K, V> record) throws InterruptedException {
        queue.put(record);
    }

    public boolean isStopped() {
        return stopped;
    }
}
