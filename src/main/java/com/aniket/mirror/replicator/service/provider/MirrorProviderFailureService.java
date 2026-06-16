package com.aniket.mirror.replicator.service.provider;

import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.kafka.FileMirroredProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class MirrorProviderFailureService {

  private final MirrorProviderRepository mirrorProviderRepository;

  private final FileMirroredProducerService mirroredProducerService;

  public void markFailed(MirrorProvider provider, String errorMessage) {
    provider.setFileStatus(FileStatus.FAILED);
    provider.setLastError(errorMessage);
    provider.setLastPolledAt(Instant.now());
    provider.setNextPollAt(null);
    mirrorProviderRepository.save(provider);
    mirroredProducerService.sendMirroredEvent(provider);
  }
}
