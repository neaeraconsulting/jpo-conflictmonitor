package us.dot.its.jpo.conflictmonitor;

import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Topology-specific Streams configs, including number of threads per topology.
 * <p>
 * The number of threads that can be useful to a topology depends on the number of
 * partitions per topic, and the topics consumed within the topology, including
 * internal/repartition topics, which varies with topology complexity.
 */
@Getter
@Setter
@Generated
@Component
@ConfigurationProperties(prefix = "streams.config")
public class TopologyStreamsConfig {

    private List<TopologyConfig> topologies;

    /**
     * @param topology Topology and consumer group name.
     * @param threads Number of threads for the topology.
     */
    public record TopologyConfig(String topology, int threads){}
}
