package io.datanerds.verteiler.it_test;

import io.datanerds.verteiler.BlockingQueueConsumer;
import io.datanerds.verteiler.it_test.producer.SimpleTestProducer;
import net._01001111.text.LoremIpsum;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.awaitility.Awaitility.await;

public class BlockingQueueConsumerTest extends EmbeddedKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueConsumerTest.class);
    private static final LoremIpsum LOREM_IPSUM = new LoremIpsum();
    private static final int NUMBER_OF_MESSAGES = 100000;
    private static final String TEST_GROUP = "TestGroup";

    private Properties props;

    @BeforeClass
    public static void setUp() {
        EmbeddedKafkaTest.setUp();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        EmbeddedKafkaTest.tearDown();
    }

    @Before
    public void setUpTest() throws Exception {
        props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaConnect);
        props.setProperty(GROUP_ID_CONFIG, TEST_GROUP);
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    @Test
    public void sendAndReceiveTest() throws Exception {
        final String topic = "my_topic";
        createTopic(topic);

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(topic, props, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic, kafkaConnect);
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
        final String topic = "my_reassignment_topic";
        createTopic(topic);

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(topic, props, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic, kafkaConnect);
        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);
        BlockingQueueConsumer<String, String> anotherConsumer = null;
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(LOREM_IPSUM.paragraph());
            if (i == NUMBER_OF_MESSAGES / 10) {
                anotherConsumer = new BlockingQueueConsumer<>(topic, props, 42, action);
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
        final String group = "OneMessageGroup";
        props.setProperty(GROUP_ID_CONFIG, group);

        createTopic(topic);

        AtomicInteger messageCounter = new AtomicInteger();
        Consumer<String> action = (message) -> messageCounter.incrementAndGet();

        BlockingQueueConsumer<String, String> consumer0 = new BlockingQueueConsumer<>(topic, props, 5, action);
        consumer0.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic, kafkaConnect);
        logger.info("Sending 1 message");

        testProducer.send(LOREM_IPSUM.paragraph());

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == 1);
        consumer0.stop();

        BlockingQueueConsumer<String, String> consumer1 = new BlockingQueueConsumer<>(topic, props, 5, action);
        consumer1.start();

        await().atMost(2, SECONDS).until(() -> messageCounter.get() == 1);
        consumer1.stop();
    }
}
