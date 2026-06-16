package com.aniket.mirror.replicator.service.polling;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MirrorPollingScheduler {

  private final MirrorPollingDispatchService dispatchService;

  /**
   * Dispatcher only.
   * Runs on Spring scheduler thread.
   * Heavy work is delegated to @Async pollingExecutor.
   */
  @Scheduled(fixedDelayString = "${mirror.polling.dispatch-delay-ms:30000}")
  public void dispatchPollingJobs() {
    log.debug("Polling tick");
    dispatchService.dispatchDuePollingJobs();
  }
}
