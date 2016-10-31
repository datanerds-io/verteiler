package io.datanerds.verteiler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Processor<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Processor.class);

    private volatile boolean stopped = false;

    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final ConsumerRecordRelay<K, V> relay;
    private final java.util.function.Consumer<V> action;

    public Processor(ConsumerRecordRelay<K, V> relay, java.util.function.Consumer<V> action, int queueSize) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.relay = relay;
        this.action = action;
    }

    @Override
    public void run() {
        try {
            while (!stopped) {
                ConsumerRecord<K, V> record = queue.take();
                action.accept(record.value());
                relay.setOffset(record);
            }
        } catch (InterruptedException ignored) {
            logger.info("Processor interrupted while waiting for messages");
        } catch (Exception ex) {
            logger.error("Exception during processing. Stopping!", ex);
        }
        queue.clear();
        logger.info("Processor stopped");
    }

    public void stop() {
        stopped = true;
    }

    public void queue(ConsumerRecord<K, V> record) throws InterruptedException {
        queue.put(record);
    }
}
