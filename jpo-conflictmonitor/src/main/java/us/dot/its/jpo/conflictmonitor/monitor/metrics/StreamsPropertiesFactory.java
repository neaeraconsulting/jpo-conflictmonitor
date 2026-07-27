package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.ConflictMonitorProperties;

import java.util.Arrays;
import java.util.Properties;

/**
 * Builds Kafka Streams {@link Properties} with {@code num.stream.threads} sized from
 * discovered input-topic partition counts (unless an explicit positive override is configured).
 */
@Component
public class StreamsPropertiesFactory {

    private static final Logger logger = LoggerFactory.getLogger(StreamsPropertiesFactory.class);

    private final ConflictMonitorProperties conflictMonitorProperties;
    private final TopicPartitionCounts topicPartitionCounts;

    public StreamsPropertiesFactory(
            ConflictMonitorProperties conflictMonitorProperties,
            TopicPartitionCounts topicPartitionCounts) {
        this.conflictMonitorProperties = conflictMonitorProperties;
        this.topicPartitionCounts = topicPartitionCounts;
    }

    /**
     * @param applicationId Streams application.id
     * @param inputTopics   primary input topics whose partition counts size stream threads
     */
    public Properties create(String applicationId, String... inputTopics) {
        Properties props = conflictMonitorProperties.createStreamProperties(applicationId);
        int threads = resolveThreadCount(applicationId, inputTopics);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        return props;
    }

    int resolveThreadCount(String applicationId, String... inputTopics) {
        int configured = conflictMonitorProperties.getStreamsConfigNumStreamThreads();
        if (configured > 0) {
            logger.info(
                    "Streams app {}: using configured num.stream.threads={} (CM_STREAMS_CONFIG_NUM_STREAM_THREADS override)",
                    applicationId, configured);
            return configured;
        }

        int discovered = topicPartitionCounts.maxPartitions(inputTopics);
        if (discovered > 0) {
            logger.info(
                    "Streams app {}: auto num.stream.threads={} from partitions of {}",
                    applicationId, discovered, Arrays.toString(inputTopics));
            return discovered;
        }

        logger.warn(
                "Streams app {}: could not discover partitions for {}; defaulting num.stream.threads=1",
                applicationId, Arrays.toString(inputTopics));
        return 1;
    }
}
