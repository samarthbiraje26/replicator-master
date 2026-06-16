package com.aniket.mirror.replicator.service.orchestrator;

import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.service.registry.MirrorProviderRegistry;
import com.aniket.mirror.replicator.service.provider.MirrorProviderFailureService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MirrorPollingOrchestrator {

  private final MirrorProviderRegistry providerRegistry;

  private final MirrorProviderFailureService failureService;

  public void orchestrate(List<MirrorProvider> mirrorProviderList) {

    for (MirrorProvider provider : mirrorProviderList) {

      try {
        String eventId = provider.getFileReplicationJob() != null ? provider.getFileReplicationJob().getEventId() : null;
        if (eventId != null && !eventId.isBlank()) {
          MDC.put("eventId", eventId);
          MDC.put("jobId", eventId);
        }
        providerRegistry
            .get(provider.getProviderName())
            .poll(provider);
      } catch (Exception ex) {
        log.error("Polling failed | provider={} | providerId={} | jobId={}",
            provider.getProviderName(), provider.getId(),
            provider.getFileReplicationJob() != null ? provider.getFileReplicationJob().getEventId() : null,
            ex);
        failureService.markFailed(provider, ex.getMessage());
      } finally {
        MDC.clear();
      }
    }
  }
}
