package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves Kafka topic partition counts via {@link KafkaAdmin} for sizing
 * {@code num.stream.threads} to match available partitions.
 */
@Component
public class TopicPartitionCounts {

    private static final Logger logger = LoggerFactory.getLogger(TopicPartitionCounts.class);

    private final KafkaAdmin kafkaAdmin;
    private final ConcurrentHashMap<String, Integer> cache = new ConcurrentHashMap<>();

    public TopicPartitionCounts(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * @return max partition count among the given topics that exist; 0 if none can be resolved
     */
    public int maxPartitions(String... topicNames) {
        if (topicNames == null || topicNames.length == 0) {
            return 0;
        }
        int max = 0;
        for (String topic : topicNames) {
            if (topic == null || topic.isBlank()) {
                continue;
            }
            int partitions = partitionsFor(topic);
            if (partitions > max) {
                max = partitions;
            }
        }
        return max;
    }

    public int partitionsFor(String topicName) {
        return cache.computeIfAbsent(topicName, this::describePartitions);
    }

    private int describePartitions(String topicName) {
        try {
            Map<String, TopicDescription> descriptions = kafkaAdmin.describeTopics(topicName);
            TopicDescription description = descriptions.get(topicName);
            if (description == null || description.partitions() == null || description.partitions().isEmpty()) {
                logger.warn("No partition metadata for topic {}", topicName);
                return 0;
            }
            int count = description.partitions().size();
            logger.info("Topic {} has {} partition(s)", topicName, count);
            return count;
        } catch (Exception e) {
            logger.warn("Failed to describe topic {}: {}", topicName, e.getMessage());
            return 0;
        }
    }

    /** Clears cached partition counts (for tests). */
    void clearCache() {
        cache.clear();
    }

    @Override
    public String toString() {
        return "TopicPartitionCounts" + Arrays.toString(cache.entrySet().toArray());
    }
}
