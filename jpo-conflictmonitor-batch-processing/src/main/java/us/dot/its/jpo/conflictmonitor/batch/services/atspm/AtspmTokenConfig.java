package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicReference;

@Configuration
public class AtspmTokenConfig {

    /**
     * Singleton holder for the current token. An AtomicReference is used, rather than a
     * shared mutable AtspmToken bean, so that refreshing the token - done independently by
     * each configured route's scheduled task, and so possibly concurrently - is a single
     * atomic swap visible to all threads, instead of several unsynchronized field writes on
     * an object other threads may be reading from at the same time.
     * @return The token holder, initially wrapping an empty token
     */
    @Bean
    public AtomicReference<AtspmToken> atspmClientToken() {
        return new AtomicReference<>(new AtspmToken());
    }

}
