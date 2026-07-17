package us.dot.its.jpo.conflictmonitor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

@Slf4j
public class LogAndContinueProcessingExceptionHandler implements ProcessingExceptionHandler {
    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> record, Exception e) {
        log.warn("Exception caught during message processing, " +
                        "processor node: {}, taskId: {}, source topic: {}, source partition: {}, source offset: {}",
                context.processorNodeId(), context.taskId(), context.topic(), context.partition(), context.offset(), e);

        return ProcessingHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {
        // No config
    }
}
