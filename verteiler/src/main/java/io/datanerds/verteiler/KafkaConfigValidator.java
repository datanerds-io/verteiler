package io.datanerds.verteiler;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

final class KafkaConfigValidator {

    private KafkaConfigValidator() {
    }

    public static void validate(Properties props) {
        verifyPropsNotNull(props);
        verifyAutoCommitDisabled(props);
    }

    private static void verifyPropsNotNull(Properties props) {
        if (props == null) {
            throw new IllegalArgumentException("Kafka properties must not be null");
        }
    }

    private static void verifyAutoCommitDisabled(Properties props) {
        Boolean isAutoCommitEnabled = Boolean.parseBoolean(props.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
        if (isAutoCommitEnabled) {
            throw new IllegalArgumentException(
                    String.format("%s must not be enabled for the BlockingQueueConsumer", ENABLE_AUTO_COMMIT_CONFIG));
        }
    }
}
