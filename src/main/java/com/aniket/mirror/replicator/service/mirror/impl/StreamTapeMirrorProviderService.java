package com.aniket.mirror.replicator.service.mirror.impl;

import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.config.properties.StreamTapeProperties;
import com.aniket.mirror.replicator.dto.response.streamtape.info.STFileInfoData;
import com.aniket.mirror.replicator.dto.response.streamtape.info.STFileInfoResponse;
import com.aniket.mirror.replicator.dto.response.streamtape.poll.STRemoteUploadPollResponse;
import com.aniket.mirror.replicator.dto.response.streamtape.poll.STVideoData;
import com.aniket.mirror.replicator.dto.response.streamtape.upload.STRemoteUploadResponse;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import com.aniket.mirror.replicator.repository.MirrorProviderRepository;
import com.aniket.mirror.replicator.service.api.impl.StreamTapeApiClient;
import com.aniket.mirror.replicator.service.kafka.FileMirroredProducerService;
import com.aniket.mirror.replicator.service.mirror.MirrorProviderService;
import com.aniket.mirror.replicator.service.s3.S3PresignService;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamTapeMirrorProviderService
    implements MirrorProviderService {


  private final StreamTapeApiClient streamTapeApiClient;

  private final S3PresignService s3PresignService;

  private final MirrorProviderRepository mirrorProviderRepository;

  private final FileMirroredProducerService mirroredProducerService;

  private final StreamTapeProperties properties;

  @Override
  public ProviderType getType() {
    return ProviderType.STREAM_TAPE;
  }

  @Async("taskExecutor")
  @Override
  public void mirror(MirrorProvider provider) {

    log.info("Mirroring via {}", getType());

    STRemoteUploadResponse response;
    String s3SignedUrl = s3PresignService.generateS3Url(provider.getFileReplicationJob());
    response =  streamTapeApiClient.upload(provider.getFileReplicationJob(),s3SignedUrl);
    provider.setPollAttemptCount(0);
    //Set when next poll should occur
    provider.setNextPollAt(Instant.now().plusSeconds(properties.getInitialPollDelaySeconds()));
    if(!validateResponse(response)) {
      log.error("Error uploading stream tape, response={}", response);
      provider.setFileStatus(FileStatus.FAILED);
      mirroredProducerService.sendMirroredEvent(provider);
    }else{
      provider.setRemoteUploadId(response.getResult().getId());
      provider.setFileStatus(FileStatus.SUBMITTED);
    }
    mirrorProviderRepository.save(provider);
    log.info("Stream tape upload response: {}", response);
    log.info("Completed mirroring via {}", getType());
  }


  @Override
  @Async("taskExecutor")
  public void checkAndRepair(MirrorProvider provider) {
    log.info("Checking and repairing for {} for external file ID {}", getType(), provider.getExternalFileId());
    
    if (provider.getExternalFileId() == null || provider.getExternalFileId().isBlank()) {
      log.warn("No external file ID found for mirror provider {}, triggering re-mirror", provider.getId());
      mirror(provider);
      return;
    }

    STFileInfoResponse response = streamTapeApiClient.getFileInfo(provider.getExternalFileId());
    
    if (response == null || !response.validateResponse()) {
      log.error("Failed to get file info from StreamTape for fileID={}, response={}", provider.getExternalFileId(), response);
      // If we can't confirm it's alive, we assume it needs re-mirroring
      mirror(provider);
      return;
    }

    STFileInfoData data = response.getResult().get(provider.getExternalFileId());
    if (data == null || data.getStatus() != 200) {
      log.warn("File {} is not alive on StreamTape (status={}), triggering re-upload", provider.getExternalFileId(), data != null ? data.getStatus() : "null");
      mirror(provider);
    } else {
      log.info("File {} is alive on StreamTape", provider.getExternalFileId());
      provider.setLastPolledAt(Instant.now());
      mirrorProviderRepository.save(provider);
      // Even if alive, send back successful event to sync status
      mirroredProducerService.sendMirroredEvent(provider);
    }
  }

  @Override
  @Async("pollingExecutor")
  public void poll(MirrorProvider job) throws InterruptedException {
    log.info("Started polling for {} for remote Id {}", getType(), job.getRemoteUploadId());
    String remoteId = job.getRemoteUploadId();

    STRemoteUploadPollResponse pollResponse;
    try {
      pollResponse = streamTapeApiClient.pollStatus(remoteId);
    } catch (Exception ex) {
      log.error("Exception while calling StreamTape poll for remoteId={}, error={}", remoteId, ex.getMessage(), ex);
      handlePollingFailure(job, "Exception during poll: " + ex.getMessage());
      mirrorProviderRepository.save(job);
      return;
    }

    if (pollResponse == null || !pollResponse.validateResponse()) {
      String err = pollResponse != null ? pollResponse.getMsg() : "Invalid/null response from StreamTape";
      log.error("Polling failed for StreamTape, remoteId={}, msg={}", remoteId, err);
      handlePollingFailure(job, err);
      mirrorProviderRepository.save(job);
      return;
    }

    STVideoData videoData = pollResponse.getResult() != null ? pollResponse.getResult().get(remoteId) : null;
    if (videoData == null) {
      log.error("No video data returned by StreamTape for remoteId={}", remoteId);
      handlePollingFailure(job, "No video data returned for id: " + remoteId);
      mirrorProviderRepository.save(job);
      return;
    }

    String status = videoData.getStatus() != null ? videoData.getStatus().toLowerCase().trim() : "unknown";

    switch (status) {
      case "finished":
        handleFinished(job, videoData);
        break;
      case "downloading":
      case "processing":
      case "queued":
      case "started":
        handleInProgress(job);
        break;
      case "error":
      case "failed":
      case "notfound":
        handlePermanentFailure(job, "Remote upload status '" + status + "' for id=" + remoteId);
        break;
      default:
        log.warn("Unknown StreamTape status '{}' for remoteId={}, treating as transient", status, remoteId);
        handleInProgress(job);
        break;
    }

    mirrorProviderRepository.save(job);
  }

  private void handleFinished(MirrorProvider job, STVideoData videoData) {
    String linkId = parseLinkId(videoData.getLinkid());
    job.setFileStatus(FileStatus.SUCCEEDED);
    job.setExternalFileId(linkId);
    job.setLastPolledAt(Instant.now());
    job.setPollAttemptCount(job.getPollAttemptCount() + 1);
    job.setNextPollAt(null);
    job.setLastError(null);
    log.info("StreamTape upload finished for remoteId={}, linkId={}", job.getRemoteUploadId(), linkId);
    mirroredProducerService.sendMirroredEvent(job);
  }

  private void handleInProgress(MirrorProvider job) {
    job.setFileStatus(FileStatus.IN_PROGRESS);
    job.setLastPolledAt(Instant.now());
    job.setPollAttemptCount(job.getPollAttemptCount() + 1);
    long nextInSeconds = calculateBackoffSeconds(job.getPollAttemptCount());
    job.setNextPollAt(Instant.now().plusSeconds(nextInSeconds));
    job.setLastError(null);
    log.info("StreamTape upload in progress for remoteId={}, next poll in {}s", job.getRemoteUploadId(), nextInSeconds);
  }

  private void handlePollingFailure(MirrorProvider job, String errorMsg) {
    job.setLastPolledAt(Instant.now());
    job.setPollAttemptCount(job.getPollAttemptCount() + 1);
    job.setLastError(errorMsg);

    if (job.getPollAttemptCount() >= properties.getPollMaxAttempts()) {
      job.setFileStatus(FileStatus.FAILED);
      job.setNextPollAt(null);
      log.error("Marking job failed after {} attempts, remoteId={}, error={}", job.getPollAttemptCount(), job.getRemoteUploadId(), errorMsg);
      mirroredProducerService.sendMirroredEvent(job);
    } else {
      long waitSeconds = calculateBackoffSeconds(job.getPollAttemptCount());
      job.setNextPollAt(Instant.now().plusSeconds(waitSeconds));
      log.info("Scheduling retry #{} for remoteId={} in {}s", job.getPollAttemptCount(), job.getRemoteUploadId(), waitSeconds);
    }
  }

  private void handlePermanentFailure(MirrorProvider job, String message) {
    job.setFileStatus(FileStatus.FAILED);
    job.setLastPolledAt(Instant.now());
    job.setPollAttemptCount(job.getPollAttemptCount() + 1);
    job.setLastError(message);
    job.setNextPollAt(null);
    log.error("Permanent failure for remoteId={}, message={}", job.getRemoteUploadId(), message);
    mirroredProducerService.sendMirroredEvent(job);
  }

  private long calculateBackoffSeconds(int attemptCount) {
    // attemptCount is 1-based
    long multiplier = 1L << Math.max(0, attemptCount - 1); // 2^(attemptCount-1)
    long seconds = properties.getBackoffBaseSeconds() * multiplier;
    return Math.min(seconds, properties.getBackoffMaxSeconds());
  }

  private String parseLinkId(Object linkId) {
    return linkId == null ? null : String.valueOf(linkId);
  }


  private boolean validateResponse(STRemoteUploadResponse response) {
    return response != null
        && response.getStatus() == 200
        && "OK".equals(response.getMsg())
        && response.getResult() != null
        && response.getResult().getId() != null;
  }


}


