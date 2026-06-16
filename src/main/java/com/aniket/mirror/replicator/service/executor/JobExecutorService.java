  package com.aniket.mirror.replicator.service.executor;

  import com.aniket.mirror.events.FileUploadEvent;
  import com.aniket.mirror.replicator.config.properties.MirrorProperties;
  import com.aniket.mirror.replicator.entity.FileReplicationJob;
  import com.aniket.mirror.replicator.service.orchestrator.MirrorUploadOrchestratorService;
  import com.aniket.mirror.replicator.service.replication.FileReplicationJobService;
  import lombok.RequiredArgsConstructor;
  import lombok.extern.slf4j.Slf4j;
  import org.springframework.scheduling.annotation.Async;
  import org.springframework.stereotype.Service;
  import org.springframework.transaction.annotation.Transactional;

  @Service
  @RequiredArgsConstructor
  @Slf4j
  public class JobExecutorService {

    private final FileReplicationJobService jobService;

    private final MirrorUploadOrchestratorService orchestrator;

    private final MirrorProperties properties;

    @Async("taskExecutor")
    @Transactional
    public void processEvent(FileUploadEvent event) {

      log.info("Processing event {}", event);

      FileReplicationJob job =
          jobService.saveFileUploadEvent(event,properties.getEnabledProviders());

      orchestrator.orchestrate(job);
    }
  }
