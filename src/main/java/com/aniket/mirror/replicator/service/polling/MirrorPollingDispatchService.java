package com.aniket.mirror.replicator.service.polling;

import com.aniket.mirror.replicator.config.properties.MirrorPollingProperties;
import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.orchestrator.MirrorPollingOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MirrorPollingDispatchService {

  private final MirrorProviderRepository mirrorProviderRepository;
  private final MirrorPollingOrchestrator mirrorPollingOrchestrator;
  private final MirrorPollingProperties properties;
  private final PlatformTransactionManager transactionManager;

  public void dispatchDuePollingJobs() {
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

    List<MirrorProvider> providers = transactionTemplate.execute(status -> {
      List<MirrorProvider> candidates = mirrorProviderRepository.findPollingCandidates(
          FileStatus.SUBMITTED.name(),
          Instant.now(),
          properties.getBatchSize()
      );

      if (candidates == null || candidates.isEmpty()) {
        return Collections.emptyList();
      }

      // Reserve the candidates so other instances do   n't pick them up
      // We push the nextPollAt 10 minutes into the future effectively "locking" it for this duration
      // The actual polling logic should update this to the real next time when it finishes
      Instant reservationTime = Instant.now().plusSeconds(600);
      candidates.forEach(mp -> mp.setNextPollAt(reservationTime));

      return mirrorProviderRepository.saveAll(candidates);
    });

    if (providers == null || providers.isEmpty()) {
      log.debug("No providers due for polling");
      return;
    }

    log.info("Dispatching polling batch | size={}", providers.size());
    mirrorPollingOrchestrator.orchestrate(providers);
  }
}
