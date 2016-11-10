package io.datanerds.verteiler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

@RunWith(MockitoJUnitRunner.class)
public class BlockingQueueConfigValidatorTest {
    private Properties testProps;

    @Before
    public void setUp() throws Exception {
        testProps = new Properties();
        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    @Test
    public void testValidate() throws Exception {
        KafkaConfigValidator.validate(testProps);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAutoCommitTrue() throws Exception {
        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        KafkaConfigValidator.validate(testProps);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateNullProperties() throws Exception {
        KafkaConfigValidator.validate(null);
    }
}
