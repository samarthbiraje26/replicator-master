package com.aniket.mirror.replicator.service.orchestrator;

import com.aniket.mirror.replicator.entity.FileReplicationJob;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.service.registry.MirrorProviderRegistry;
import com.aniket.mirror.replicator.service.provider.MirrorProviderFailureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MirrorUploadOrchestratorService {

  private final MirrorProviderRegistry providerRegistry;

  private final MirrorProviderFailureService failureService;


  public void orchestrate(FileReplicationJob job) {

    for (MirrorProvider provider : job.getMirrorProviderList()) {

          try {
        if (job.getEventId() != null && !job.getEventId().isBlank()) {
          MDC.put("eventId", job.getEventId());
          MDC.put("jobId", job.getEventId());
        }
        providerRegistry
            .get(provider.getProviderName())
            .mirror(provider);
      } catch (Exception ex) {
        log.error("Mirror failed | provider={} | providerId={} | jobId={}",
            provider.getProviderName(), provider.getId(), job.getEventId(), ex);
        failureService.markFailed(provider, ex.getMessage());
      } finally {
        MDC.clear();
      }
    }
  }
}
