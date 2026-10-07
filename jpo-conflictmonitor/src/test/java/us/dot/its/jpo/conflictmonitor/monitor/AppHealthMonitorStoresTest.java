package us.dot.its.jpo.conflictmonitor.monitor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import us.dot.its.jpo.conflictmonitor.monitor.models.bsm.BsmIntersectionIdKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.map.MapIndex;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.IntersectionEventTopology;
import us.dot.its.jpo.conflictmonitor.testutils.BsmTestUtils;
import us.dot.its.jpo.conflictmonitor.testutils.SpatTestUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppHealthMonitorStoresTest {

    private AppHealthMonitor controller;
    private IntersectionEventTopology topology;

    @Before
    public void setUp() {
        controller = new AppHealthMonitor();
        topology = mock(IntersectionEventTopology.class);
        controller.setIntersectionEventTopology(topology);
    }

    @Test
    public void spatialReturnsAllIndexedItemsWithJsonResponse() {
        MapIndex mapIndex = new MapIndex();
        Object first = new Object();
        Object second = new Object();
        mapIndex.getQuadtree().insert(new Envelope(0, 1, 0, 1), first);
        mapIndex.getQuadtree().insert(new Envelope(2, 3, 2, 3), second);
        controller.setMapIndex(mapIndex);

        ResponseEntity<List> response = controller.spatial();

        assertJsonOk(response);
        assertEquals(2, response.getBody().size());
        assertTrue(response.getBody().contains(first));
        assertTrue(response.getBody().contains(second));
    }

    @Test
    public void spatialReturnsEmptyListWhenIndexIsEmpty() {
        controller.setMapIndex(new MapIndex());

        ResponseEntity<List> response = controller.spatial();

        assertJsonOk(response);
        assertTrue(response.getBody().isEmpty());
    }

    @Test
    public void spatialThrowsWhenMapIndexIsMissing() {
        controller.setMapIndex(null);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> controller.spatial());

        assertEquals("The map index is null", exception.getMessage());
    }

    @Test
    public void mapStoreUsesIntersectionKeyStringsAndClosesIterator() {
        ReadOnlyKeyValueStore<RsuIntersectionKey, ProcessedMap<LineString>> store = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<RsuIntersectionKey, ProcessedMap<LineString>> iterator = mock(KeyValueIterator.class);
        RsuIntersectionKey firstKey = new RsuIntersectionKey("rsu-a", 1001);
        RsuIntersectionKey secondKey = new RsuIntersectionKey("rsu-b", 1001);
        ProcessedMap<LineString> firstMap = new ProcessedMap<>();
        ProcessedMap<LineString> secondMap = new ProcessedMap<>();
        when(topology.getMapStore()).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(new KeyValue<>(firstKey, firstMap), new KeyValue<>(secondKey, secondMap));

        ResponseEntity<TreeMap<String, ProcessedMap<LineString>>> response = controller.mapStore();

        assertJsonOk(response);
        assertEquals(2, response.getBody().size());
        assertSame(firstMap, response.getBody().get(firstKey.toString()));
        assertSame(secondMap, response.getBody().get(secondKey.toString()));
        verify(iterator).close();
    }

    @Test
    public void spatWindowStoreGroupsByIntersectionWindowAndRsuAndClosesIterator() {
        ReadOnlyWindowStore<RsuIntersectionKey, ProcessedSpat> store = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<RsuIntersectionKey>, ProcessedSpat> iterator = mock(KeyValueIterator.class);
        RsuIntersectionKey intersectionOneFirstRsu = new RsuIntersectionKey("rsu-a", 1001);
        RsuIntersectionKey intersectionOneSecondRsu = new RsuIntersectionKey("rsu-b", 1001);
        RsuIntersectionKey intersectionTwoRsu = new RsuIntersectionKey("rsu-c", 2002);
        ProcessedSpat firstWindowFirstRsu = SpatTestUtils.validSpat(1001);
        ProcessedSpat firstWindowSecondRsu = SpatTestUtils.validSpat(1001);
        ProcessedSpat secondWindowSpat = SpatTestUtils.validSpat(1001);
        ProcessedSpat secondIntersection = SpatTestUtils.validSpat(2002);
        long firstWindowStart = Instant.parse("2025-01-02T03:04:05Z").toEpochMilli();
        long secondWindowStart = Instant.parse("2025-01-02T03:05:05Z").toEpochMilli();
        when(topology.getSpatWindowStore()).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, true, false);
        when(iterator.next()).thenReturn(
                new KeyValue<>(windowed(intersectionOneFirstRsu, firstWindowStart), firstWindowFirstRsu),
                new KeyValue<>(windowed(intersectionOneSecondRsu, firstWindowStart), firstWindowSecondRsu),
                new KeyValue<>(windowed(intersectionOneFirstRsu, secondWindowStart), secondWindowSpat),
                new KeyValue<>(windowed(intersectionTwoRsu, firstWindowStart), secondIntersection));

        ResponseEntity<AppHealthMonitor.IntersectionSpatMap> response = controller.spatWindowStore();

        assertJsonOk(response);
        AppHealthMonitor.IntersectionSpatMap intersections = response.getBody();
        assertEquals(2, intersections.size());
        TreeMap<String, TreeMap<String, ProcessedSpat>> intersectionOne = intersections.get(1001);
        TreeMap<String, TreeMap<String, ProcessedSpat>> intersectionTwo = intersections.get(2002);
        String firstWindow = "2025-01-02T03:04:05Z / 2025-01-02T03:04:35Z";
        String secondWindowLabel = "2025-01-02T03:05:05Z / 2025-01-02T03:05:35Z";
        assertEquals(2, intersectionOne.size());
        assertEquals(2, intersectionOne.get(firstWindow).size());
        assertSame(firstWindowFirstRsu, intersectionOne.get(firstWindow).get(intersectionOneFirstRsu.toString()));
        assertSame(firstWindowSecondRsu, intersectionOne.get(firstWindow).get(intersectionOneSecondRsu.toString()));
        assertEquals(1, intersectionOne.get(secondWindowLabel).size());
        assertSame(secondWindowSpat, intersectionOne.get(secondWindowLabel).get(intersectionOneFirstRsu.toString()));
        assertEquals(1, intersectionTwo.size());
        assertEquals(1, intersectionTwo.get(firstWindow).size());
        assertSame(secondIntersection, intersectionTwo.get(firstWindow).get(intersectionTwoRsu.toString()));
        verify(iterator).close();
    }

    @Test
    public void bsmWindowStoreGroupsByVehicleWindowAndIntersectionKeyAndClosesIterator() {
        ReadOnlyWindowStore<BsmIntersectionIdKey, ProcessedBsm<Point>> store = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<BsmIntersectionIdKey>, ProcessedBsm<Point>> iterator = mock(KeyValueIterator.class);
        BsmIntersectionIdKey firstVehicleFirstRsu = new BsmIntersectionIdKey("key-bsm-a", "rsu-a", 1001, "log-a");
        BsmIntersectionIdKey firstVehicleSecondRsu = new BsmIntersectionIdKey("key-bsm-b", "rsu-b", 1001, "log-a");
        BsmIntersectionIdKey secondVehicleRsu = new BsmIntersectionIdKey("key-bsm-c", "rsu-c", 2002, "log-b");
        ProcessedBsm<Point> firstWindowFirstRsu = bsm("vehicle-a");
        ProcessedBsm<Point> firstWindowSecondRsu = bsm("vehicle-a");
        ProcessedBsm<Point> secondWindowBsm = bsm("vehicle-a");
        ProcessedBsm<Point> secondVehicle = bsm("vehicle-b");
        long firstWindowStart = Instant.parse("2025-01-02T03:04:05Z").toEpochMilli();
        long secondWindowStart = Instant.parse("2025-01-02T03:05:05Z").toEpochMilli();
        when(topology.getBsmWindowStore()).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, true, false);
        when(iterator.next()).thenReturn(
                new KeyValue<>(windowed(firstVehicleFirstRsu, firstWindowStart), firstWindowFirstRsu),
                new KeyValue<>(windowed(firstVehicleSecondRsu, firstWindowStart), firstWindowSecondRsu),
                new KeyValue<>(windowed(firstVehicleFirstRsu, secondWindowStart), secondWindowBsm),
                new KeyValue<>(windowed(secondVehicleRsu, firstWindowStart), secondVehicle));

        ResponseEntity<AppHealthMonitor.IntersectionBsm> response = controller.bsmWindowStore();

        assertJsonOk(response);
        AppHealthMonitor.IntersectionBsm vehicles = response.getBody();
        assertEquals(2, vehicles.size());
        TreeMap<String, TreeMap<String, ProcessedBsm<Point>>> vehicleOne = vehicles.get("vehicle-a");
        TreeMap<String, TreeMap<String, ProcessedBsm<Point>>> vehicleTwo = vehicles.get("vehicle-b");
        String firstWindow = "2025-01-02T03:04:05Z / 2025-01-02T03:04:35Z";
        String secondWindowLabel = "2025-01-02T03:05:05Z / 2025-01-02T03:05:35Z";
        assertEquals(2, vehicleOne.size());
        assertEquals(2, vehicleOne.get(firstWindow).size());
        assertSame(firstWindowFirstRsu, vehicleOne.get(firstWindow).get(firstVehicleFirstRsu.toString()));
        assertSame(firstWindowSecondRsu, vehicleOne.get(firstWindow).get(firstVehicleSecondRsu.toString()));
        assertEquals(1, vehicleOne.get(secondWindowLabel).size());
        assertSame(secondWindowBsm, vehicleOne.get(secondWindowLabel).get(firstVehicleFirstRsu.toString()));
        assertEquals(1, vehicleTwo.size());
        assertEquals(1, vehicleTwo.get(firstWindow).size());
        assertSame(secondVehicle, vehicleTwo.get(firstWindow).get(secondVehicleRsu.toString()));
        verify(iterator).close();
    }

    @Test
    public void emptyStoresReturnEmptyBodiesAndCloseIterators() {
        ReadOnlyKeyValueStore<RsuIntersectionKey, ProcessedMap<LineString>> mapStore = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<RsuIntersectionKey, ProcessedMap<LineString>> mapIterator = mock(KeyValueIterator.class);
        ReadOnlyWindowStore<RsuIntersectionKey, ProcessedSpat> spatStore = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<RsuIntersectionKey>, ProcessedSpat> spatIterator = mock(KeyValueIterator.class);
        ReadOnlyWindowStore<BsmIntersectionIdKey, ProcessedBsm<Point>> bsmStore = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<BsmIntersectionIdKey>, ProcessedBsm<Point>> bsmIterator = mock(KeyValueIterator.class);
        when(topology.getMapStore()).thenReturn(mapStore);
        when(mapStore.all()).thenReturn(mapIterator);
        when(mapIterator.hasNext()).thenReturn(false);
        when(topology.getSpatWindowStore()).thenReturn(spatStore);
        when(spatStore.all()).thenReturn(spatIterator);
        when(spatIterator.hasNext()).thenReturn(false);
        when(topology.getBsmWindowStore()).thenReturn(bsmStore);
        when(bsmStore.all()).thenReturn(bsmIterator);
        when(bsmIterator.hasNext()).thenReturn(false);

        ResponseEntity<TreeMap<String, ProcessedMap<LineString>>> mapResponse = controller.mapStore();
        ResponseEntity<AppHealthMonitor.IntersectionSpatMap> spatResponse = controller.spatWindowStore();
        ResponseEntity<AppHealthMonitor.IntersectionBsm> bsmResponse = controller.bsmWindowStore();

        assertJsonOk(mapResponse);
        assertJsonOk(spatResponse);
        assertJsonOk(bsmResponse);
        assertTrue(mapResponse.getBody().isEmpty());
        assertTrue(spatResponse.getBody().isEmpty());
        assertTrue(bsmResponse.getBody().isEmpty());
        verify(mapIterator).close();
        verify(spatIterator).close();
        verify(bsmIterator).close();
    }

    @Test
    public void allStoreIteratorsCloseWhenIterationThrows() {
        ReadOnlyKeyValueStore<RsuIntersectionKey, ProcessedMap<LineString>> mapStore = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<RsuIntersectionKey, ProcessedMap<LineString>> mapIterator = mock(KeyValueIterator.class);
        ReadOnlyWindowStore<RsuIntersectionKey, ProcessedSpat> spatStore = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<RsuIntersectionKey>, ProcessedSpat> spatIterator = mock(KeyValueIterator.class);
        ReadOnlyWindowStore<BsmIntersectionIdKey, ProcessedBsm<Point>> bsmStore = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Windowed<BsmIntersectionIdKey>, ProcessedBsm<Point>> bsmIterator = mock(KeyValueIterator.class);
        when(topology.getMapStore()).thenReturn(mapStore);
        when(mapStore.all()).thenReturn(mapIterator);
        when(topology.getSpatWindowStore()).thenReturn(spatStore);
        when(spatStore.all()).thenReturn(spatIterator);
        when(topology.getBsmWindowStore()).thenReturn(bsmStore);
        when(bsmStore.all()).thenReturn(bsmIterator);
        when(mapIterator.hasNext()).thenReturn(true);
        when(spatIterator.hasNext()).thenReturn(true);
        when(bsmIterator.hasNext()).thenReturn(true);
        IllegalStateException mapFailure = new IllegalStateException("map iteration failed");
        IllegalStateException spatFailure = new IllegalStateException("spat iteration failed");
        IllegalStateException bsmFailure = new IllegalStateException("bsm iteration failed");
        doThrow(mapFailure).when(mapIterator).next();
        doThrow(spatFailure).when(spatIterator).next();
        doThrow(bsmFailure).when(bsmIterator).next();

        assertSame(mapFailure, assertThrows(IllegalStateException.class, () -> controller.mapStore()));
        assertSame(spatFailure, assertThrows(IllegalStateException.class, () -> controller.spatWindowStore()));
        assertSame(bsmFailure, assertThrows(IllegalStateException.class, () -> controller.bsmWindowStore()));

        verify(mapIterator).close();
        verify(spatIterator).close();
        verify(bsmIterator).close();
    }

    private static void assertJsonOk(ResponseEntity<?> response) {
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(MediaType.APPLICATION_JSON, response.getHeaders().getContentType());
    }

    private static ProcessedBsm<Point> bsm(String vehicleId) {
        ProcessedBsm<Point> bsm = BsmTestUtils.validProcessedBsm();
        bsm.getProperties().setId(vehicleId);
        return bsm;
    }

    private static <K> Windowed<K> windowed(K key, long startTime) {
        return new Windowed<>(key, new TimeWindow(startTime, startTime + 30_000));
    }
}
