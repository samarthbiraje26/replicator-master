package com.aniket.mirror.replicator.service.mirror.impl;

import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.mirror.MirrorProviderService;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KrakenFileMirrorProviderService implements MirrorProviderService {

  private final MirrorProviderRepository mirrorProviderRepository;

  @Override
  public ProviderType getType() {
    return ProviderType.KRAKEN_FILE;
  }

  @Async("taskExecutor")
  @Override
  public void mirror(MirrorProvider provider) {

    log.info("Mirroring via {}", getType());

    log.info("Completed mirroring via {}", getType());
  }

  @Override
  @Async("pollingExecutor")
  public void poll(MirrorProvider job) throws InterruptedException{

  }

  @Override
  @Async("taskExecutor")
  public void checkAndRepair(MirrorProvider job) throws InterruptedException {
    log.warn("checkAndRepair not implemented for KrakenFile");
    job.setLastPolledAt(Instant.now());
    mirrorProviderRepository.save(job);
  }


}
