package us.dot.its.jpo.conflictmonitor.monitor.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

public class IntersectionVehicleRequestKeySerde implements Serde<IntersectionVehicleRequestKey> {
    @Override
    public Serializer<IntersectionVehicleRequestKey> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<IntersectionVehicleRequestKey> deserializer() {
        return new JsonDeserializer<>(IntersectionVehicleRequestKey.class);
    }
}
