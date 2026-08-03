package us.dot.its.jpo.ode.messagesender;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


import java.util.Map;

@RestController
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);



    @Autowired
    KafkaTemplate<String, String> template;


    public static final String X_KAFKA_KEY = "x-kafka-key";

    @PostMapping(value = "/kafka/{topic}", consumes = "*/*", produces = "*/*")
    public @ResponseBody ResponseEntity<String> kafka(@RequestBody String message, @PathVariable String topic, @RequestHeader Map<String, String> headers) {
        try {
            var sb = new StringBuilder();
            headers.forEach((k, v) -> sb.append(String.format("%s: %s%n", k.toLowerCase(), v)));
            logger.info("Headers:");
            logger.info(sb.toString());
            SendResult<String, String> sendResult = null;
            if (headers.containsKey(X_KAFKA_KEY)) {
                String key = headers.get(X_KAFKA_KEY);
                logger.info("Found Kafka key in header: {}", key);
                var result = template.send(topic, key, message);
                sendResult = result.join();
            } else {
                var result = template.send(topic, message);
                sendResult = result.join();
            }
            String strResult = sendResult.toString();
            logger.info("Send Result: {}", sendResult);
            return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body(strResult);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.TEXT_PLAIN)
                    .body(ExceptionUtils.getStackTrace(e));
        }

    }

    

//    @PostMapping(value = "/script", consumes = "*/*", produces = "*/*")
//    public @ResponseBody ResponseEntity<String> runScript(@RequestBody String script) {
//        logger.info("runScript");
//        try {
//            scriptRunner.scheduleScript(script);
//            return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body("running script");
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.TEXT_PLAIN)
//                    .body(ExceptionUtils.getStackTrace(e));
//        }
//    }

    
   
   
}
