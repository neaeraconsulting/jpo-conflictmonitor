package us.dot.its.jpo.ode.messagesender.scriptrunner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ApplicationEventListener {
    @EventListener
    public void onAllEvents(ApplicationEvent event) {
        log.info("Received application event {}", event);
    }
}
