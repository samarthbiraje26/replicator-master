package com.aniket.mirror.replicator.service.kafka;

import com.aniket.mirror.events.FileMirrorCheckEvent;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.registry.MirrorProviderRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MirrorCheckConsumer {

  private final MirrorProviderRepository mirrorProviderRepository;
  private final MirrorProviderRegistry providerRegistry;

  @KafkaListener(
      topics = "${mirror.kafka.file-mirror-check-topic:file_mirror_check}",
      groupId = "mirror-check-group"
  )
  public void consume(FileMirrorCheckEvent event) {
    log.info("Received FileMirrorCheckEvent: {}", event);

    MirrorProvider provider = mirrorProviderRepository
        .findByFileReplicationJob_File_FileIdAndProviderName(
            event.getFileId(),
            com.aniket.mirror.replicator.constants.ProviderType.valueOf(event.getProviderName())
        ).orElse(null);

    if (provider == null) {
      log.error("Mirror provider record not found for fileId: {}, provider: {}", event.getFileId(), event.getProviderName());
      return;
    }

    try {
      providerRegistry.get(provider.getProviderName()).checkAndRepair(provider);
    } catch (InterruptedException e) {
      log.error("Check and repair interrupted for fileId: {}", event.getFileId(), e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("Error during check and repair for fileId: {}", event.getFileId(), e);
    }
  }
}
