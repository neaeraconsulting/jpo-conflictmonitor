package us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.geojsonconverter.standards.MapStandard;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@Generated
public class MapBroadcastRateEvent
    extends BroadcastRateEvent {

        public MapBroadcastRateEvent() {
            super("MapBroadcastRate");
        }

        private MapStandard standard;
        private TimestampType timestampType;

    }
