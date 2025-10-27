package us.dot.its.jpo.ode.messagesender.scriptrunner.hex;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import us.dot.its.jpo.ode.messagesender.scriptrunner.DateJsonMapper;

/**
 * Listens to Kafka ODE JSON topics and constructs a script.
 */
@Component
@Slf4j
public class KafkaListeners {



    final KafkaTemplate<String, String> kafkaTemplate;
    File outputFile;
    long startTime;
    boolean placeholders;
    File mapFile;
    File spatFile;
    File bsmFile;
    File rtcmFile;
    File srmFile;
    File ssmFile;

    @Autowired
    public KafkaListeners(KafkaTemplate<String, String> kafkaTemplate) {
        log.info("KafkaListeners constructor");
        this.kafkaTemplate = kafkaTemplate;
    }


    public void startSavingToFile(File outputFile, long startTime, boolean placeholders,
            File mapFile, File spatFile, File bsmFile, File rtcmFile, File srmFile, File ssmFile) {
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
    }


    @KafkaListener(topics = "topic.OdeBsmJson", groupId = "hexLogConverter-bsm")
    void listenBsm(String message) {
        listen(DSRCmsgID.BSM, message);
    }

    @KafkaListener(topics = "topic.OdeSpatJson", groupId = "hexLogConverter-spat")
    void listenSpat(String message) {
        listen(DSRCmsgID.SPAT, message);
    }

    @KafkaListener(topics = "topic.OdeMapJson", groupId = "hexLogConverter-map")
    void listenMap(String message) {
        listen(DSRCmsgID.MAP, message);
    }

    @KafkaListener(topics = "topic.OdeRtcmJson", groupId = "hexLogConverter-rtcm")
    void listenRtcm(String message) {
        listen(DSRCmsgID.RTCM, message);
    }

    @KafkaListener(topics = "topic.OdeSrmJson", groupId = "hexLogConverter-srm")
    void listenSrm(String message) {
        listen(DSRCmsgID.SRM, message);
    }

    @KafkaListener(topics = "topic.OdeSsmJson", groupId = "hexLogConverter-ssm")
    void listenSsm(String message) {
        listen(DSRCmsgID.SSM, message);
    }

    void listen(DSRCmsgID msgId, String message)  {
        final long now = System.currentTimeMillis();
        final long offsetTime = now - startTime;
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
            msgFile = ssmFile;
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
                JsonNode data = node.at("/payload/data");
                ((ObjectNode)data).put("timeStamp", MINUTE_OF_YEAR);
                JsonNode intersectionList = node.at("/payload/data/intersectionStateList/intersectionStatelist");
                if (intersectionList.isArray()) {
                    for (JsonNode intersection : intersectionList) {
                        ObjectNode intersectionObj = (ObjectNode)intersection;
                        intersectionObj.put("moy", MINUTE_OF_YEAR);
                        intersectionObj.put("timeStamp", MILLI_OF_MINUTE);
                    }
                }
            } else if (DSRCmsgID.BSM.equals(msgId)) {
                JsonNode coreData = node.at("/payload/data/coreData");
                ObjectNode coreDataObj = (ObjectNode)coreData;
                coreDataObj.put("secMark", MILLI_OF_MINUTE);
                coreDataObj.put("id", TEMP_ID);
            }
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return message;
        }
    }

}
