package com.aniket.mirror.replicator.service.kafka;

import com.aniket.mirror.events.FileMirrorCheckEvent;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MirrorCheckProducerService {

  private final KafkaTemplate<String, Object> kafkaTemplate;

  @Value("${mirror.kafka.file-mirror-check-topic:file_mirror_check}")
  private String topic;

  public void sendCheckEvent(MirrorProvider provider) {
    if (provider == null || provider.getFileReplicationJob() == null) {
      log.warn("Cannot send mirror check event: provider or job is null");
      return;
    }

    FileMirrorCheckEvent event = new FileMirrorCheckEvent(
        provider.getFileReplicationJob().getFile().getFileId(),
        provider.getProviderName().name()
    );

    log.info("Sending FileMirrorCheckEvent | fileId={} | provider={}",
        event.getFileId(), event.getProviderName());

    kafkaTemplate.send(topic, event.getFileId(), event);
  }
}
