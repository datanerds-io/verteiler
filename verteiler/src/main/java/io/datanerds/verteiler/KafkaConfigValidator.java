package io.datanerds.verteiler;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

final class KafkaConfigValidator {
    static final String NULL_PROPS_MESSAGE = "Kafka properties must not be null";
    static final String AUTO_COMMIT_ENABLED_MSG =
            String.format("%s must not be enabled for the BlockingQueueConsumer", ENABLE_AUTO_COMMIT_CONFIG);

    private KafkaConfigValidator() {
    }

    public static void validate(Properties props) {
        verifyPropsNotNull(props);
        verifyAutoCommitDisabled(props);
    }

    private static void verifyPropsNotNull(Properties props) {
        if (props == null) {
            throw new IllegalArgumentException(NULL_PROPS_MESSAGE);
        }
    }

    private static void verifyAutoCommitDisabled(Properties props) {
        Boolean isAutoCommitEnabled = Boolean.parseBoolean(props.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
        if (isAutoCommitEnabled) {
            throw new IllegalArgumentException(AUTO_COMMIT_ENABLED_MSG);
        }
    }
}
