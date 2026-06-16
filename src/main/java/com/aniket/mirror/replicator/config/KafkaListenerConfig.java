package com.aniket.mirror.replicator.config;

import com.aniket.mirror.replicator.config.properties.KafkaProcessingProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;

@Configuration
@RequiredArgsConstructor
public class KafkaListenerConfig {

  private final KafkaProcessingProperties properties;

  @Bean
  public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

    long backoffMs = properties.getRetry().getBackoffMs();
    int maxAttempts = properties.getRetry().getMaxAttempts();
    long maxFailures = Math.max(0, maxAttempts - 1L);

    DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(backoffMs, maxFailures));
    errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
    return errorHandler;
  }

  /**
   * Best-effort correlation from Kafka headers into MDC.
   *
   * Note: listener methods can also set MDC (especially for eventId/jobId).
   */
  public static void putHeaderIfPresent(ConsumerRecord<?, ?> record, String header, String mdcKey) {
    if (record == null || record.headers() == null) {
      return;
    }
    var h = record.headers().lastHeader(header);
    if (h == null || h.value() == null) {
      return;
    }
    MDC.put(mdcKey, new String(h.value(), StandardCharsets.UTF_8));
  }
}
