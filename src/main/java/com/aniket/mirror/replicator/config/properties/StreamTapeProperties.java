package com.aniket.mirror.replicator.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mirror.stream-tape")
@Data
public class StreamTapeProperties {

  /** API login for StreamTape. */
  private String apiLogin;

  /** API key for StreamTape. */
  private String apiKey;

  /** Initial delay before first poll after submitting remote upload. */
  private long initialPollDelaySeconds = 10;

  /** Total polling attempts before marking FAILED. */
  private int pollMaxAttempts = 3;

  /** Base seconds for exponential backoff. */
  private long backoffBaseSeconds = 60;

  /** Max backoff seconds. */
  private long backoffMaxSeconds = 30 * 60;
}
