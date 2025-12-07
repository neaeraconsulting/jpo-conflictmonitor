package us.dot.its.jpo.ode.messagesender.scriptrunner.hex;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import us.dot.its.jpo.ode.messagesender.scriptrunner.DateJsonMapper;

/**
 * Listens to Kafka ODE JSON topics and constructs a script.
 */
@Component
@DependsOn({"kafkaTemplate", "kafkaConfig"})
@Slf4j
public class KafkaListeners {

    File outputFile;
    long startTime;
    boolean placeholders;
    File mapFile;
    File spatFile;
    File bsmFile;
    File rtcmFile;
    File srmFile;
    File ssmFile;
    boolean immediate;
    Integer accelerate;

    @Getter
    private final KafkaListenerEndpointRegistry registry;


    @Autowired
    public KafkaListeners(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }


    public void startSavingToFile(File outputFile, long startTime, boolean placeholders,
            File mapFile, File spatFile, File bsmFile, File rtcmFile, File srmFile, File ssmFile,
                                  boolean immediate, Integer accelerate) {
        log.info("KafkaListener: startSavingToFile");
        this.outputFile = outputFile;
        this.startTime = startTime;
        this.placeholders = placeholders;
        this.mapFile = mapFile;
        this.spatFile = spatFile;
        this.bsmFile = bsmFile;
        this.rtcmFile = rtcmFile;
        this.srmFile = srmFile;
        this.ssmFile = ssmFile;
        this.immediate = immediate;
        this.accelerate = accelerate;
    }


    @KafkaListener(topics = {"topic.OdeSpatJson"}, groupId = "#{spatGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenSpat(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.SPAT, record, metadata);
    }

    @KafkaListener(topics = {"topic.OdeMapJson"}, groupId = "#{mapGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenMap(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.MAP, record, metadata);
    }

    @KafkaListener(topics = {"topic.OdeBsmJson"}, groupId = "#{bsmGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenBsm(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.BSM, record, metadata);
    }

    @KafkaListener(topics = {"topic.OdeRtcmJson"}, groupId = "#{rtcmGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenRtcm(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.RTCM, record, metadata);
    }

    @KafkaListener(topics = {"topic.OdeSrmJson"}, groupId = "#{srmGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenSrm(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.SRM, record, metadata);
    }

    @KafkaListener(topics = {"topic.OdeSsmJson"}, groupId = "#{ssmGroup}",
            containerFactory = "kafkaListenerContainerFactory")
    void listenSsm(ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata) {
        listen(DSRCmsgID.SSM, record, metadata);
    }


    AtomicLong counter = new AtomicLong(1L);

    void listen(DSRCmsgID msgId, ConsumerRecord<String, String> record, ConsumerRecordMetadata metadata)  {
        log.info("received record msgId: {}, timestamp: {}", msgId, metadata.timestamp());
        final String message = record.value();
        final long now = System.currentTimeMillis();

        final long actualOffsetTime = now - startTime;
        final long offsetTime = accelerate != null ? actualOffsetTime/accelerate : actualOffsetTime;

        log.info("{}: Received {} message", offsetTime, msgId);
         
        if (outputFile != null) {
            if (offsetTime < 0) {
                log.info("Not saving old message with negative offset time");
                return;
            }
            var templatedMessage = placeholders ? substitutePlaceholders(msgId, message) : message;
            var formattedMessage = String.format("%s,%s,%s%n", msgId, offsetTime, templatedMessage);  
            if (!outputFile.exists()) {
                try {
                    outputFile.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException("Error creating file", e);
                }
            }
            log.info("{}: Writing {} message to file {}", offsetTime, msgId, outputFile.getName());
            try {
                FileUtils.writeStringToFile(outputFile, formattedMessage, StandardCharsets.UTF_8, true);
            } catch (IOException e) {
                log.error("Error writing to file", e);
            }
        } else {
            log.info("Output file is null");
        }
        writeToMessageFile(msgId, message, offsetTime);
    }

    void writeToMessageFile(DSRCmsgID msgId, String message, long offsetTime) {
        File msgFile = null;
        if (msgId == DSRCmsgID.BSM) {
            msgFile = bsmFile;
        } else if (msgId == DSRCmsgID.SPAT) {
            msgFile = spatFile;
        } else if (msgId == DSRCmsgID.MAP) {
            msgFile = mapFile;
        } else if (msgId == DSRCmsgID.RTCM) {
            msgFile = rtcmFile;
        } else if (msgId == DSRCmsgID.SRM) {
            msgFile = srmFile;
        } else if (msgId == DSRCmsgID.SSM) {
            msgFile = ssmFile;
        }
        if (msgFile == null) return;


        if (!msgFile.exists()) {
            try {
                msgFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Error creating file", e);
            }
        }
        log.info("{}: Writing {} message to file {}", offsetTime, msgId, msgFile.getName());
        String formattedMessage = String.format("%s%n", message);
        try {
            FileUtils.writeStringToFile(msgFile, formattedMessage, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            log.error("Error writing to file", e);
        }

    }

    final String ISO_DATE_TIME = "@ISO_DATE_TIME@";
    final String MINUTE_OF_YEAR = "@MINUTE_OF_YEAR@";
    final String MILLI_OF_MINUTE = "@MILLI_OF_MINUTE@";
    final String TEMP_ID = "@TEMP_ID@";

    String substitutePlaceholders(DSRCmsgID msgId, String message) {
        var mapper = DateJsonMapper.getInstance();
        try {
            JsonNode node = mapper.readTree(message);
            JsonNode metadata = node.at("/metadata");
            ((ObjectNode)metadata).put("odeReceivedAt", ISO_DATE_TIME);
            if (DSRCmsgID.SPAT.equals(msgId)) {
//                JsonNode data = node.at("/payload/data");
//                ((ObjectNode)data).put("timeStamp", MINUTE_OF_YEAR);
//                JsonNode intersectionList = node.at("/payload/data/intersectionStateList/intersectionStatelist");
//                if (intersectionList.isArray()) {
//                    for (JsonNode intersection : intersectionList) {
//                        ObjectNode intersectionObj = (ObjectNode)intersection;
//                        intersectionObj.put("moy", MINUTE_OF_YEAR);
//                        intersectionObj.put("timeStamp", MILLI_OF_MINUTE);
//                    }
//                }
                substituteSpatPlaceholders(node);
            } else if (DSRCmsgID.BSM.equals(msgId)) {
//                JsonNode coreData = node.at("/payload/data/coreData");
//                ObjectNode coreDataObj = (ObjectNode) coreData;
//                coreDataObj.put("secMark", MILLI_OF_MINUTE);
//                coreDataObj.put("id", TEMP_ID);
                substituteBsmPlaceholders(node);
            } else if (DSRCmsgID.RTCM.equals(msgId)) {
                substituteRtcmPlaceholders(node);
            } else if (DSRCmsgID.SRM.equals(msgId)) {
                substituteSrmPlaceholders(node);
            } else if (DSRCmsgID.SSM.equals(msgId)) {
                substituteSsmPlaceholders(node);
            }
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return message;
        }
    }

    private void substituteSpatPlaceholders(JsonNode node) {
    }

    private void substituteBsmPlaceholders(JsonNode node) {
    }

    private void substituteSrmPlaceholders(JsonNode node) {
        JsonNode request = node.at("/payload/data/value/SignalRequestMessage");
        if (request instanceof ObjectNode requestObj) {
            requestObj.put("timeStamp", MINUTE_OF_YEAR);
            requestObj.put("second", MILLI_OF_MINUTE);
        } else {
            log.error("node not found");
        }
    }

    private void substituteSsmPlaceholders(JsonNode node) {
        JsonNode status = node.at("/payload/data/value/SignalStatusMessage");
        if (status instanceof ObjectNode statusObj) {
            statusObj.put("timeStamp", MINUTE_OF_YEAR);
            statusObj.put("second", MILLI_OF_MINUTE);
        } else {
            log.error("node not found");
        }
    }

    private void substituteRtcmPlaceholders(JsonNode node) {
        // TODO
    }
}
