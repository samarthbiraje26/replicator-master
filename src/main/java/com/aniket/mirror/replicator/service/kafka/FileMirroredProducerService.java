package com.aniket.mirror.replicator.service.kafka;

import com.aniket.mirror.events.FileMirroredEvent;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileMirroredProducerService {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  @Value("${mirror.kafka.file-mirrored-topic:file_mirrored}")
  private String topic;

  public void sendMirroredEvent(MirrorProvider provider) {
    if (provider == null || provider.getFileReplicationJob() == null) {
      log.warn("Cannot send mirrored event: provider or job is null");
      return;
    }

    FileMirroredEvent event = new FileMirroredEvent(
        provider.getFileReplicationJob().getFile().getFileId(),
        provider.getProviderName().name(),
        provider.getFileStatus().name(),
        provider.getExternalFileId(),
        provider.getLastError(),
        Instant.now()
    );

    log.info("Sending FileMirroredEvent | fileId={} | provider={} | status={}",
        event.getFileId(), event.getProviderName(), event.getStatus());

    kafkaTemplate.send(topic, event.getFileId(), event);
  }
}
