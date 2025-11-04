package us.dot.its.jpo.conflictmonitor.monitor.processors.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;

/**
 * Processor to keep track of stream time per metrics key, and send tick events
 * to metrics to prevent windows with sparse events from not closing and being missed.
 */
@Slf4j
public class TickProcessor<TKey>
    extends ContextualProcessor<TKey, String, TKey, String> {

    /**
     * String constant representing a tick to advance event stream time
     */
    public static String TICK = "TICK";

    final Duration metricsInterval;
    final Duration punctuateInterval;
    final Duration retentionTime;
    final boolean isDebug;
    final String metricName;
    final String timestampStoreName;
    KeyValueStore<TKey, Timestamps> timeStore;

    public TickProcessor(CommonMetricsParameters params, boolean isDebug, String metricName, String timestampStoreName) {
        metricsInterval = Duration.of(params.getInterval(), params.getIntervalUnits());
        punctuateInterval = Duration.of(params.getCheckInterval(), params.getCheckIntervalUnits());
        retentionTime = Duration.of(params.getRetentionTime(), params.getRetentionTimeUnits());
        this.isDebug = isDebug;
        this.metricName = metricName;
        this.timestampStoreName = timestampStoreName;
    }

    @Override
    public void init(ProcessorContext<TKey, String> context) {
        super.init(context);
        this.timeStore = context.getStateStore(timestampStoreName);
        context.schedule(punctuateInterval, PunctuationType.WALL_CLOCK_TIME,
                this::punctuate);
    }

    @Override
    public void process(Record<TKey, String> record) {
        // Keep track of stream time and clock time per key
        TKey key = record.key();
        long streamTime = context().currentStreamTimeMs();
        long clockTime = context().currentSystemTimeMs();
        var timestamps = new Timestamps(streamTime, clockTime);
        timeStore.put(key, timestamps);
    }

    private void punctuate(final long punctuateClockTime) {
        if (isDebug) {
            log.debug("punctuate at {} for {}", punctuateClockTime, metricName);
        }
        var keysToDelete = new HashSet<TKey>();
        try (var storeIterator = timeStore.all()) {
            while (storeIterator.hasNext()) {
                KeyValue<TKey, Timestamps> item = storeIterator.next();
                TKey key = item.key;
                Timestamps timestamps = item.value;
                final long streamTime = timestamps.streamTime();
                final long clockTime = timestamps.clockTime();

                // Punctuate timestamp is clock time
                final long millisSinceLastMessage = punctuateClockTime - clockTime;
                final Duration timeSinceLastMessage = Duration.ofMillis(millisSinceLastMessage);
                if (timeSinceLastMessage.isNegative()) {
                    log.error("For key {}, punctuate time, {}, is before clock time of latest message, {}.  " +
                                    "Something is wrong with the clock.  Deleting this key/value",
                            key, punctuateClockTime, clockTime);
                    keysToDelete.add(key);
                    continue;
                }

                // Check if elapsed time is longer than the metrics aggregation interval,
                // and emit a tick if so
                if (timeSinceLastMessage.compareTo(metricsInterval) >= 0) {
                    // Advance stream time by the same amount of time that clock time has advanced
                    final long newStreamTime = streamTime + millisSinceLastMessage;
                    final var record = new Record<>(key, TICK, newStreamTime);
                    context().forward(record);
                    if (isDebug) {
                        log.info("emitted tick: key: {}, value {}, timestamp {}",
                                record.key(), record.value(), record.timestamp());
                    }
                }

                // Check if elapsed time is longer than the retention time, delete if so
                if (timeSinceLastMessage.compareTo(retentionTime) >= 0) {
                    keysToDelete.add(key);
                }
            }
        }
        // Clean up
        for (TKey key : keysToDelete) {
            timeStore.delete(key);
        }

    }

    /**
     * Record to store stream time and clock time for one same message
     * @param streamTime The stream time
     * @param clockTime The clock time
     */
    public record Timestamps(long streamTime, long clockTime){
    }

    public static Serde<Timestamps> TimestampsSerdes() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Timestamps.class));
    }
}
