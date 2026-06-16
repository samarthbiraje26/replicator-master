package com.aniket.mirror.replicator.service.kafka;

import com.aniket.mirror.events.FileUploadEvent;
import com.aniket.mirror.replicator.config.KafkaListenerConfig;
import com.aniket.mirror.replicator.service.replication.FileReplicationJobService;
import com.aniket.mirror.replicator.service.executor.JobExecutorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class UploadEventConsumer
{

  private final FileReplicationJobService fileReplicationJobService;

  private final JobExecutorService jobExecutorService;

  @KafkaListener(
      topics = "${mirror.kafka.file-upload-topic:file_upload}",
      groupId = "${spring.kafka.consumer.group-id:file-upload-consumer-group}"
  )
  public void consume(ConsumerRecord<String, FileUploadEvent> record, Acknowledgment ack) {
    FileUploadEvent event = record.value();

    try {
      KafkaListenerConfig.putHeaderIfPresent(record, "X-Trace-Id", "traceId");
      KafkaListenerConfig.putHeaderIfPresent(record, "X-Event-Id", "eventId");

      if (event != null && event.getEventId() != null && !event.getEventId().isBlank()) {
        MDC.put("eventId", event.getEventId());
        MDC.put("jobId", event.getEventId());
      }

      log.info("Received FileUploadEvent | key={} | topic={} | partition={} | offset={}",
          record.key(), record.topic(), record.partition(), record.offset());

      FileReplicationJobService.ValidationResult validation = fileReplicationJobService.validateFileUploadEvent(event);

      if (validation == FileReplicationJobService.ValidationResult.INVALID) {
        throw new IllegalArgumentException("Invalid FileUploadEvent");
      }
      if (validation == FileReplicationJobService.ValidationResult.DUPLICATE) {
        log.info("Duplicate FileUploadEvent ignored (idempotent) | eventId={} | fileId={}", event.getEventId(), event.getFileId());
        ack.acknowledge();
        return;
      }

      processEvent(event);
      ack.acknowledge();
    } finally {
      MDC.clear();
    }
  }


  //process event here
  private void processEvent(FileUploadEvent event) {
    log.info("Processing FileUploadEvent: {}", event);
    jobExecutorService.processEvent(event);
    log.info("End of processing FileUploadEvent");
  }



}
