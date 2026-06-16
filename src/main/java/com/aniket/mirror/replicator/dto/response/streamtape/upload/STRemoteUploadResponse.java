package com.aniket.mirror.replicator.dto.response.streamtape.upload;

import com.aniket.mirror.replicator.dto.response.ApiResponse;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;


@Getter
@Setter
@NoArgsConstructor
@ToString
public class STRemoteUploadResponse extends ApiResponse {
  private int status;
  private String msg;
  private STRemoteUploadRequestResult result;
}
