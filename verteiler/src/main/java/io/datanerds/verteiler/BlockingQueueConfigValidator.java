package io.datanerds.verteiler;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

final class BlockingQueueConfigValidator implements KafkaConfigValidator {
    private static volatile BlockingQueueConfigValidator validator;

    static BlockingQueueConfigValidator getValidator() {
        if (validator == null) {
            synchronized (BlockingQueueConfigValidator.class) {
                if (validator == null) {
                    validator = new BlockingQueueConfigValidator();
                }
            }
        }

        return validator;
    }

    private BlockingQueueConfigValidator() {}

    @Override
    public void validate(Properties props) {
        verifyPropsNotNull(props);
        verifyAutoCommitDisabled(props);
    }

    private void verifyPropsNotNull(Properties props) {
        if (props == null) {
            throw new IllegalArgumentException("Kafka properties must not be null");
        }
    }

    private void verifyAutoCommitDisabled(Properties props) {
        Boolean isAutoCommitEnabled = Boolean.valueOf(props.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
        if (isAutoCommitEnabled) {
            throw new IllegalArgumentException(
                    ENABLE_AUTO_COMMIT_CONFIG + " must not be enabled for the BlockingQueueConsumer");
        }
    }
}
