package com.aniket.mirror.replicator.dto.response.streamtape.upload;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class STRemoteUploadRequestResult {
  private String id;
  private String folderid;
}