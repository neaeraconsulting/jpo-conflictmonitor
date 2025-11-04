package us.dot.its.jpo.conflictmonitor.monitor.processors.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.Event;

import java.time.Duration;

/**
 * Processor to keep track of stream time per metrics key, and send tick events
 * to metrics to prevent windows with sparse events from not closing and being missed.
 */
@Slf4j
public class TickProcessor<TKey>
    extends ContextualProcessor<TKey, String, TKey, String> {

    final Duration punctuateInterval;
    final boolean isDebug;
    final String metricName;

    public TickProcessor(Duration punctuateInterval, boolean isDebug, String metricName) {
        this.punctuateInterval = punctuateInterval;
        this.isDebug = isDebug;
        this.metricName = metricName;
    }

    @Override
    public void init(ProcessorContext<TKey, String> context) {
        super.init(context);
        context.schedule(punctuateInterval, PunctuationType.WALL_CLOCK_TIME,
                this::punctuate);
    }

    @Override
    public void process(Record<TKey, String> record) {
        // Keep track of stream time per partition
        int partition = context().taskId().partition();
    }

    private void punctuate(final long timestamp) {
        if (isDebug) {
            log.debug("punctuate at {} for {}", timestamp, metricName);
        }

    }
}
