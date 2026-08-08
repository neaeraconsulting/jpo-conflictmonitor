package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.EvictingQueue;

import java.io.IOException;
import java.util.*;

@JsonSerialize(using = TimestampBoundedQueue.SpatBoundedQueueSerializer.class)
@JsonDeserialize(using = TimestampBoundedQueue.SpatBoundedQueueDeserializer.class)
public class TimestampBoundedQueue {

    private final EvictingQueue<Long> queue;

    public static final int MAX_SIZE = 10;

    public TimestampBoundedQueue() {
        queue = EvictingQueue.create(MAX_SIZE);
    }

    @JsonCreator
    public TimestampBoundedQueue(Collection<Long> collection) {
        queue = EvictingQueue.create(MAX_SIZE);
        queue.addAll(collection);
    }

    public void add(long value) {
        queue.offer(value);
    }

    public int size() {
        return queue.size();
    }

    public List<Long> toList() {
        return new ArrayList<>(queue);
    }

    /**
     * Get the most recently added pair of timestamps
     * @return two timestamps or empty if there aren't two timestamps in the queue
     */
    public Optional<long[]> pair() {
        if (size() < 2) {
            return Optional.empty();
        }
        List<Long> list = toList();
        long[] pair = new long[2];
        pair[0] = list.get(0);
        pair[1] = list.get(1);
        return Optional.of(pair);
    }

    public Optional<long[]> all() {
        if (size() < MAX_SIZE) {
            return Optional.empty();
        }
        return Optional.of(toList().stream().mapToLong(x -> x).toArray());
    }

    public static class SpatBoundedQueueSerializer extends JsonSerializer<TimestampBoundedQueue> {
        @Override
        public void serialize(TimestampBoundedQueue value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (value == null) return;
            List<Long> list = value.toList();
            serializers.defaultSerializeValue(list, gen);
        }
    }

    public static class SpatBoundedQueueDeserializer extends JsonDeserializer<TimestampBoundedQueue> {
        @Override
        public TimestampBoundedQueue deserialize(JsonParser parser, DeserializationContext context) throws IOException, JacksonException {
           var typeRef = new TypeReference<List<Long>>() {};
            return parser.readValueAs(typeRef);
        }
    }

}
