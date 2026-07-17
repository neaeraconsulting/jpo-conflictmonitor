package us.dot.its.jpo.conflictmonitor.monitor.models.Intersection;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gates alignment event emission so unchanged MAP/SPaT identifier sets are not
 * re-emitted on every SPaT, while still allowing a periodic sample heartbeat.
 */
public class AlignmentEmitGate {

    private final ConcurrentHashMap<String, Snapshot> lastEmitted = new ConcurrentHashMap<>();
    private final long sampleIntervalMs;

    public AlignmentEmitGate(long sampleIntervalMs) {
        this.sampleIntervalMs = sampleIntervalMs > 0 ? sampleIntervalMs : 10_000L;
    }

    /**
     * @return true if an alignment event should be emitted for this key
     */
    public boolean shouldEmit(String key, Object setA, Object setB, long nowMs) {
        Snapshot previous = lastEmitted.get(key);
        if (previous == null) {
            lastEmitted.put(key, new Snapshot(setA, setB, nowMs));
            return true;
        }
        boolean changed = !Objects.equals(previous.setA, setA) || !Objects.equals(previous.setB, setB);
        boolean sampleDue = nowMs - previous.emittedAtMs >= sampleIntervalMs;
        if (changed || sampleDue) {
            lastEmitted.put(key, new Snapshot(setA, setB, nowMs));
            return true;
        }
        return false;
    }

    public void clear() {
        lastEmitted.clear();
    }

    private static final class Snapshot {
        private final Object setA;
        private final Object setB;
        private final long emittedAtMs;

        private Snapshot(Object setA, Object setB, long emittedAtMs) {
            // Defensive copy for Sets to avoid mutation after storage
            this.setA = copyIfSet(setA);
            this.setB = copyIfSet(setB);
            this.emittedAtMs = emittedAtMs;
        }

        private static Object copyIfSet(Object value) {
            if (value instanceof Set<?> set) {
                return Set.copyOf(set);
            }
            return value;
        }
    }
}
