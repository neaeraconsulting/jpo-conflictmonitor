package us.dot.its.jpo.conflictmonitor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysContinueProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(AlwaysContinueProductionExceptionHandler.class);

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Production exception sending to topic {}, continuing: ", record.topic(), exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }
}