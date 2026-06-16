package com.aniket.mirror.replicator.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mirror.polling")
@Data
public class MirrorPollingProperties {

  /** How often the scheduler attempts to dispatch polling work. */
  private long dispatchDelayMs = 30_000;

  /** Max number of providers to poll per dispatch tick. */
  private int batchSize = 50;
}
