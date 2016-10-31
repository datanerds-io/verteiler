package io.datanerds.verteiler.it_test;

import io.datanerds.verteiler.BlockingQueueConsumer;
import io.datanerds.verteiler.ConsumerConfig;
import io.datanerds.verteiler.it_test.producer.SimpleTestProducer;
import net._01001111.text.LoremIpsum;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class BlockingQueueConsumerTest extends EmbeddedKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumerTest.class);
    private static final LoremIpsum LOREM_IPSUM = new LoremIpsum();
    private static final String TOPIC = "my_topic";
    private static final int NUMBER_OF_MESSAGES = 50;

    @BeforeClass
    public static void setUp() {
        EmbeddedKafkaTest.setUp();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        EmbeddedKafkaTest.tearDown();
    }

    @Test
    public void sendAndReceiveTest() throws Exception {
        createTopic(TOPIC);

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        ConsumerConfig<String, String> config =
                new ConsumerConfig<>(kafkaConnect, "TestGroup", TOPIC, new StringDeserializer(),
                        new StringDeserializer());
        BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(config, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", TOPIC, kafkaConnect);
        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(LOREM_IPSUM.paragraph());
        }

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == NUMBER_OF_MESSAGES);
        testProducer.close();
        consumer.stop();
    }

}
