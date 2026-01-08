package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.BroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.RtcmBroadcastRateEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

import java.time.Duration;

/**
 * Base class for zero broadcast rate checker. Maintains a state store with the latest wall clock timestamp for each
 * key, and emits events if the current time is more than the rolling window period later.
 * @param <TItem> Type of input items, ProcessedMap or ProcessedSpat
 * @param <TEvent> Type of output events
 */
public abstract class BaseZeroRateChecker<TItem, TEvent extends BroadcastRateEvent, TKey>
    implements Processor<TKey, TItem, TKey, TEvent> {

    protected abstract Logger getLogger();

    protected abstract TEvent createEvent();

    private KeyValueStore<TKey, Long> store;
    private final int maxAgeMillis;
    private final int checkEveryMillis;
    private final String inputTopicName;
    private final String stateStoreName;

    ProcessorContext<TKey, TEvent> context;

    public BaseZeroRateChecker(final int rollingPeriodSeconds, final int outputIntervalSeconds, final String inputTopicName, final String stateStoreName) {
        this.maxAgeMillis = rollingPeriodSeconds * 1000;
        this.checkEveryMillis = outputIntervalSeconds * 1000;
        this.inputTopicName = inputTopicName;
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<TKey, TEvent> context) {
        this.context = context;
        this.store = context.getStateStore(stateStoreName);
        context.schedule(Duration.ofMillis(checkEveryMillis),
                PunctuationType.WALL_CLOCK_TIME,
                this::punctuate);
    }

    @Override
    public void process(Record<TKey, TItem> record) {
        store.put(record.key(), System.currentTimeMillis());
    }

    private void punctuate(long timestamp) {
        // Check if any keys are older than the max age
        try (var storeIterator = store.all()) {
            while (storeIterator.hasNext()) {
                KeyValue<TKey, Long> item =storeIterator.next();
                TKey key = item.key;
                Long lastTimestamp = item.value;
                if (timestamp - lastTimestamp > maxAgeMillis) {
                    emitZeroEvent(key, timestamp);
                    // Remove the key to avoid sending redundant zero rate events forever
                    store.delete(key);
                }
            }
        }
    }

    private void emitZeroEvent(TKey key, long timestamp) {
        getLogger().info("emit zero rate event for key = {} at timestamp = {}", key, timestamp);
        TEvent event = createEvent();
        event.setSource(key.toString());
        if (key instanceof RsuIntersectionKey intersectionKey) {
            event.setIntersectionID(intersectionKey.getIntersectionId());
            event.setRoadRegulatorID(intersectionKey.getRegion());
        } else if (event instanceof RtcmBroadcastRateEvent rtcmEvent && key instanceof RsuStationIdKey) {
            rtcmEvent.setStationId(rtcmEvent.getStationId());
        }
        event.setTopicName(inputTopicName);
        ProcessingTimePeriod timePeriod = new ProcessingTimePeriod();
        timePeriod.setBeginTimestamp(timestamp - maxAgeMillis);
        timePeriod.setEndTimestamp(timestamp);
        event.setTimePeriod(timePeriod);
        event.setNumberOfMessages(0);
        context.forward(new Record<>(key, event, timestamp));
    }
}
