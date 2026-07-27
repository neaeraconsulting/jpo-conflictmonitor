package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Intercepts GET /actuator/prometheus before Actuator content-negotiation.
 * <p>
 * This app uses {@code @EnableWebMvc}, so Actuator's OpenMetrics negotiation can still
 * own /actuator/prometheus and return HTTP 500 even when {@code registry.scrape()} works.
 * Serving Prometheus text format here avoids that path entirely.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class PrometheusScrapeFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusScrapeFilter.class);
    private static final String PATH = "/actuator/prometheus";
    private static final String CONTENT_TYPE = "text/plain;version=0.0.4;charset=utf-8";

    private final MeterRegistry meterRegistry;
    private final Optional<PrometheusMeterRegistry> prometheusMeterRegistry;

    public PrometheusScrapeFilter(
            MeterRegistry meterRegistry,
            Optional<PrometheusMeterRegistry> prometheusMeterRegistry) {
        this.meterRegistry = meterRegistry;
        this.prometheusMeterRegistry = prometheusMeterRegistry;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"GET".equalsIgnoreCase(request.getMethod())
                || !PATH.equals(request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        PrometheusMeterRegistry prometheus = resolvePrometheusRegistry();
        if (prometheus == null) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain;charset=utf-8");
            response.getWriter().write("No PrometheusMeterRegistry available");
            return;
        }
        try {
            byte[] body = prometheus.scrape().getBytes(StandardCharsets.UTF_8);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
            response.setContentType(CONTENT_TYPE);
            response.setContentLength(body.length);
            response.getOutputStream().write(body);
        } catch (Exception e) {
            logger.error("Prometheus text scrape failed for {}", PATH, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain;charset=utf-8");
            response.getWriter().write(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private PrometheusMeterRegistry resolvePrometheusRegistry() {
        if (prometheusMeterRegistry.isPresent()) {
            return prometheusMeterRegistry.get();
        }
        if (meterRegistry instanceof PrometheusMeterRegistry prometheus) {
            return prometheus;
        }
        if (meterRegistry instanceof CompositeMeterRegistry composite) {
            for (MeterRegistry child : composite.getRegistries()) {
                if (child instanceof PrometheusMeterRegistry prometheus) {
                    return prometheus;
                }
            }
        }
        return null;
    }
}
