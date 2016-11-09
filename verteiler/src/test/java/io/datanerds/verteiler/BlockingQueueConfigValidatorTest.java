package io.datanerds.verteiler;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@RunWith(MockitoJUnitRunner.class)
public class BlockingQueueConfigValidatorTest {
    private BlockingQueueConfigValidator validator;
    private Properties testProps;

    @Before
    public void setUp() throws Exception {
        validator = BlockingQueueConfigValidator.getValidator();

        testProps = new Properties();
        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    @Test
    public void testGetValidator() throws Exception {
        final BlockingQueueConfigValidator other = BlockingQueueConfigValidator.getValidator();
        assertThat(validator, is(equalTo(other)));
    }

    @Test
    public void testValidate() throws Exception {
        validator.validate(testProps);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAutoCommitTrue() throws Exception {
        testProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
        validator.validate(testProps);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateNullProperties() throws Exception {
        validator.validate(null);
    }
}
