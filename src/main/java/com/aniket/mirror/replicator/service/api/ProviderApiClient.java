package com.aniket.mirror.replicator.service.api;

import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.dto.response.ApiResponse;
import com.aniket.mirror.replicator.entity.FileReplicationJob;

public interface ProviderApiClient {
  ProviderType getType();

  ApiResponse upload(FileReplicationJob job,String fileURL);

  ApiResponse pollStatus(String remoteUploadId);

  ApiResponse getFileInfo(String externalFileId);
}
