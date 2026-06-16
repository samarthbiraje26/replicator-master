package com.aniket.mirror.replicator.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "mirror.recheck")
@Data
public class MirrorRecheckProperties {

  /**
   * Period after which a file should be rechecked if not polled.
   */
  private int periodDays = 30;

  /**
   * Cron expression for the recheck job.
   * Default: Twice a day at midnight and noon.
   */
  private String cron = "0 0 0,12 * * *";

  /**
   * Max number of providers to pick for recheck per run.
   */
  private int batchSize = 100;
}
