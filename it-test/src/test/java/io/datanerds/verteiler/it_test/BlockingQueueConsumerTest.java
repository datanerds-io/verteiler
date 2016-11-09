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
    private static final int NUMBER_OF_MESSAGES = 100000;
    private static final String TEST_GROUP = "TestGroup";

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
        createTopic("my_topic");

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        ConsumerConfig<String, String> config =
                new ConsumerConfig<>(kafkaConnect, TEST_GROUP, "my_topic", new StringDeserializer(),
                        new StringDeserializer());
        BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(config, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", "my_topic", kafkaConnect);
        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(LOREM_IPSUM.paragraph());
        }

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == NUMBER_OF_MESSAGES);
        testProducer.close();
        consumer.stop();
    }

    @Test
    public void reassignmentTest() throws Exception {
        createTopic("my_reassignment_topic");
        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        ConsumerConfig<String, String> config =
                new ConsumerConfig<>(kafkaConnect, TEST_GROUP, "my_reassignment_topic", new StringDeserializer(),
                        new StringDeserializer());
        BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(config, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", "my_reassignment_topic", kafkaConnect);
        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);
        BlockingQueueConsumer<String, String> anotherConsumer = null;
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(LOREM_IPSUM.paragraph());
            if (i == NUMBER_OF_MESSAGES / 10) {
                anotherConsumer = new BlockingQueueConsumer<>(
                        new ConsumerConfig<>(kafkaConnect, TEST_GROUP, "my_reassignment_topic",
                                new StringDeserializer(), new StringDeserializer()), 42, action);
                anotherConsumer.start();
            }
            if (i == NUMBER_OF_MESSAGES / 5) {
                consumer.stop();
            }
        }

        await().atMost(5, SECONDS).until(() -> messageCounter.get() >= NUMBER_OF_MESSAGES);
        testProducer.close();
        anotherConsumer.stop();
    }

    @Test
    public void testSendOneMessageRestartConsumerEnsureOneMessageOnly() throws Exception {
        final String topic = "low_load_topic";

        createTopic(topic);

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        ConsumerConfig<String, String> config =
                new ConsumerConfig<>(kafkaConnect, TEST_GROUP, topic, new StringDeserializer(),
                        new StringDeserializer());

        BlockingQueueConsumer<String, String> consumer0 = new BlockingQueueConsumer<>(config, 5, action);
        consumer0.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic, kafkaConnect);
        logger.info("Sending 1 message");

        testProducer.send(LOREM_IPSUM.paragraph());

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == 1);
        consumer0.stop();

        BlockingQueueConsumer<String, String> consumer1 = new BlockingQueueConsumer<>(config, 5, action);
        consumer1.start();

        await().atMost(2, SECONDS).until(() -> messageCounter.get() == 1);
        consumer1.stop();
    }
}
