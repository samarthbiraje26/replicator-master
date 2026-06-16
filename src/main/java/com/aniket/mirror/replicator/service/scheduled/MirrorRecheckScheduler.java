package com.aniket.mirror.replicator.service.scheduled;

import com.aniket.mirror.replicator.config.properties.MirrorRecheckProperties;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.kafka.MirrorCheckProducerService;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
@RequiredArgsConstructor
public class MirrorRecheckScheduler {

  private final MirrorProviderRepository mirrorProviderRepository;
  private final MirrorCheckProducerService mirrorCheckProducerService;
  private final MirrorRecheckProperties properties;

  @Scheduled(cron = "${mirror.recheck.cron:0 0 0,12 * * *}")
  @Transactional
  public void runRecheckSweep() {
    log.info("Starting global recheck sweep for providers not polled in {} days", properties.getPeriodDays());

    Instant threshold = Instant.now().minus(properties.getPeriodDays(), ChronoUnit.DAYS);
    List<MirrorProvider> providersToRecheck = mirrorProviderRepository.findRecheckCandidates(
        threshold,
        properties.getBatchSize()
    );

    if (providersToRecheck.isEmpty()) {
      log.info("No providers found for recheck");
      return;
    }

    log.info("Found {} providers for recheck", providersToRecheck.size());

    for (MirrorProvider provider : providersToRecheck) {
      try {
        log.info("Triggering recheck for provider id={}, fileId={}", provider.getId(), provider.getFileReplicationJob().getFile().getFileId());
        
        // Update lastPolledAt to current time so it's not picked up again immediately
        provider.setLastPolledAt(Instant.now());
        mirrorProviderRepository.save(provider);

        // Send Kafka event to trigger actual check
        mirrorCheckProducerService.sendCheckEvent(provider);
      } catch (Exception e) {
        log.error("Failed to trigger recheck for provider id={}: {}", provider.getId(), e.getMessage());
      }
    }

    log.info("Completed global recheck sweep");
  }
}
