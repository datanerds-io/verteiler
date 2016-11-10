package io.datanerds.verteiler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static io.datanerds.verteiler.KafkaConfigValidator.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

@RunWith(MockitoJUnitRunner.class)
public class KafkaConfigValidatorTest {
    private Properties testProps;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        testProps = new Properties();
        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    @Test
    public void testValidate() throws Exception {
        validate(testProps);
    }

    @Test
    public void testValidateAutoCommitTrue() throws Exception {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage(AUTO_COMMIT_ENABLED_MSG);

        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        validate(testProps);
    }

    @Test
    public void testValidateNullProperties() throws Exception {
        expected.expect(IllegalArgumentException.class);
        expected.expectMessage(NULL_PROPS_MESSAGE);

        validate(null);
    }
}
