package com.aniket.mirror.replicator.dto.response.streamtape.info;

import com.aniket.mirror.replicator.dto.response.ApiResponse;
import java.util.Map;
import lombok.Data;

@Data
public class STFileInfoResponse extends ApiResponse {
  private int status;
  private String msg;
  private Map<String, STFileInfoData> result;

  public boolean validateResponse() {
    return status == 200 && "OK".equals(msg) && result != null;
  }
}
