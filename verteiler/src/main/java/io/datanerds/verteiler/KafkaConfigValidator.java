package io.datanerds.verteiler;

import java.util.Properties;

interface KafkaConfigValidator {
    /**
     * Validates whether the given Kafka configuration is suitable for Consumer configuration.  Depending on the
     * Consumer type, some kafka fields should not be changed.
     *
     * @param props    Kafka client properties
     */
    void validate(Properties props);
}
