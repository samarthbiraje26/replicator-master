package com.aniket.mirror.replicator.service.api.impl;

import com.aniket.mirror.replicator.config.properties.StreamTapeProperties;
import com.aniket.mirror.replicator.exception.ServerException;
import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.dto.response.ApiResponse;
import com.aniket.mirror.replicator.dto.response.streamtape.info.STFileInfoResponse;
import com.aniket.mirror.replicator.dto.response.streamtape.poll.STRemoteUploadPollResponse;
import com.aniket.mirror.replicator.dto.response.streamtape.upload.STRemoteUploadResponse;
import com.aniket.mirror.replicator.entity.FileReplicationJob;
import com.aniket.mirror.replicator.service.api.ProviderApiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

@Slf4j
@Component
public class StreamTapeApiClient
    implements ProviderApiClient {

  private final RestClient restClient;

  private final StreamTapeProperties properties;

  public StreamTapeApiClient(RestClient.Builder builder, StreamTapeProperties properties) {
    this.properties = properties;
    this.restClient = builder
        .baseUrl("https://api.streamtape.com")
        .build();
  }
  @Override
  public ProviderType getType() {
    return ProviderType.STREAM_TAPE;
  }

  @Override
  public STRemoteUploadResponse upload(FileReplicationJob job,String fileURL) {
    long startTime = System.currentTimeMillis();
    try {
      if (properties.getApiLogin() == null || properties.getApiLogin().isBlank()
          || properties.getApiKey() == null || properties.getApiKey().isBlank()) {
        throw new IllegalStateException("StreamTape credentials are not configured");
      }
      log.info("Starting external call to StreamTape API for upload, jobId: {}", job.getEventId());
      STRemoteUploadResponse response = restClient.get()
          .uri(uriBuilder -> uriBuilder
              .path("/remotedl/add")
              .queryParam("login", properties.getApiLogin())
              .queryParam("key", properties.getApiKey())
              .queryParam("url", fileURL)
              .build()
          )
          .retrieve()
          .body(STRemoteUploadResponse.class);

      long duration = System.currentTimeMillis() - startTime;
      log.info("External call to StreamTape API for upload completed successfully in {}ms, jobId: {}", duration, job.getEventId());
      return response;
    } catch (RestClientResponseException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape upload call | jobId={} | durationMs={} | status={} | response={} ", job.getEventId(), duration, ex.getStatusCode(), truncateResponseBody(ex.getResponseBodyAsString()));
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to initiate remote upload via StreamTape API", ex);
    } catch (RestClientException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape upload call | jobId={} | durationMs={} | error={} ", job.getEventId(), duration, ex.getMessage(), ex);
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to initiate remote upload via StreamTape API", ex);
    }
  }

  @Override
  public STRemoteUploadPollResponse pollStatus(String remoteUploadId) {
    long startTime = System.currentTimeMillis();
    try {
      if (properties.getApiLogin() == null || properties.getApiLogin().isBlank()
          || properties.getApiKey() == null || properties.getApiKey().isBlank()) {
        throw new IllegalStateException("StreamTape credentials are not configured");
      }
      log.info("Starting external call to StreamTape API for poll status, uploadId: {}", remoteUploadId);
      STRemoteUploadPollResponse response = restClient.get()
          .uri(uriBuilder -> uriBuilder
          .path("/remotedl/status")
          .queryParam("login", properties.getApiLogin())
          .queryParam("key", properties.getApiKey())
          .queryParam("id", remoteUploadId)
          .build()
            )
            .retrieve()
          .body(STRemoteUploadPollResponse.class);

      long duration = System.currentTimeMillis() - startTime;
      log.info("External call to StreamTape API for poll status completed successfully in {}ms, uploadId: {}", duration, remoteUploadId);
      return response;
    } catch (RestClientResponseException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape poll call | uploadId={} | durationMs={} | status={} | response={}", remoteUploadId, duration, ex.getStatusCode(), truncateResponseBody(ex.getResponseBodyAsString()));
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to poll status from StreamTape API", ex);
    } catch (RestClientException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape poll call | uploadId={} | durationMs={} | error={}", remoteUploadId, duration, ex.getMessage(), ex);
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to poll status from StreamTape API", ex);
    }
  }

  @Override
  public STFileInfoResponse getFileInfo(String externalFileId) {
    long startTime = System.currentTimeMillis();
    try {
      if (properties.getApiLogin() == null || properties.getApiLogin().isBlank()
          || properties.getApiKey() == null || properties.getApiKey().isBlank()) {
        throw new IllegalStateException("StreamTape credentials are not configured");
      }
      log.info("Starting external call to StreamTape API for file info, fileId: {}", externalFileId);
      STFileInfoResponse response = restClient.get()
          .uri(uriBuilder -> uriBuilder
              .path("/file/info")
              .queryParam("login", properties.getApiLogin())
              .queryParam("key", properties.getApiKey())
              .queryParam("file", externalFileId)
              .build()
          )
          .retrieve()
          .body(STFileInfoResponse.class);

      long duration = System.currentTimeMillis() - startTime;
      log.info("External call to StreamTape API for file info completed successfully in {}ms, fileId: {}", duration, externalFileId);
      return response;
    } catch (RestClientResponseException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape file info call | fileId={} | durationMs={} | status={} | response={}", externalFileId, duration, ex.getStatusCode(), truncateResponseBody(ex.getResponseBodyAsString()));
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to get file info from StreamTape API", ex);
    } catch (RestClientException ex) {
      long duration = System.currentTimeMillis() - startTime;
      log.error("Failed StreamTape file info call | fileId={} | durationMs={} | error={}", externalFileId, duration, ex.getMessage(), ex);
      throw new ServerException("EXTERNAL_SERVICE_FAILURE", HttpStatus.BAD_GATEWAY, "Failed to get file info from StreamTape API", ex);
    }
  }

  private String truncateResponseBody(String body) {
    if (body == null) return null;
    return body.length() > 500 ? body.substring(0, 500) + "..." : body;
  }
}
