package com.aniket.mirror.replicator.service.replication;

import com.aniket.mirror.events.FileUploadEvent;
import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.entity.FileReplicationJob;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.FileReplicationJobRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileReplicationJobService {

  private final FileReplicationJobRepository repository;

  public ValidationResult validateFileUploadEvent(FileUploadEvent event) {
    if (event == null) {
      return ValidationResult.INVALID;
    }
    if (event.getEventId() == null || event.getEventId().isBlank()) {
      return ValidationResult.INVALID;
    }
    if (event.getFileId() == null || event.getFileId().isBlank()) {
      return ValidationResult.INVALID;
    }
    if (event.getFileName() == null || event.getFileName().isBlank()) {
      return ValidationResult.INVALID;
    }

    if (repository.existsById(event.getEventId()) || repository.existsByFile_FileId(event.getFileId())) {
      return ValidationResult.DUPLICATE;
    }
    return ValidationResult.NEW;
  }

  public enum ValidationResult {
    NEW,
    DUPLICATE,
    INVALID
  }

  public FileReplicationJob saveFileUploadEvent(FileUploadEvent event,
      List<ProviderType> mirrorProviders) {

    FileReplicationJob fileReplicationJob = new FileReplicationJob(event);
    //Create Mirror provider entity objects
    List<MirrorProvider> mirrorProviderEntities = mirrorProviders.stream().map(msp->{
          MirrorProvider mp = new MirrorProvider();
          mp.setProviderName(msp);
          mp.setFileStatus(FileStatus.CREATED);
          mp.setFileReplicationJob(fileReplicationJob);
          return mp;
        }).toList();

    fileReplicationJob.setMirrorProviderList(mirrorProviderEntities);
    return repository.save(fileReplicationJob);
  }
}
