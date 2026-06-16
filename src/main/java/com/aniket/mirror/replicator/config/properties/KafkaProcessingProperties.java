package com.aniket.mirror.replicator.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mirror.kafka")
@Data
public class KafkaProcessingProperties {

  /** Topic containing file upload events published by the upload service. */
  private String fileUploadTopic = "file_upload";

  private Retry retry = new Retry();

  @Data
  public static class Retry {
    /** Total delivery attempts including the first try. */
    private int maxAttempts = 5;

    /** Backoff between attempts in milliseconds. */
    private long backoffMs = 1000;
  }
}
