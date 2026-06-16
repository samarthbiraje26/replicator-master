package com.aniket.mirror.replicator.dto.response.streamtape.poll;


import com.aniket.mirror.replicator.dto.response.ApiResponse;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class STRemoteUploadPollResponse extends ApiResponse {

  private int status;

  private String msg;

  private Map<String, STVideoData> result;

  public boolean validateResponse() {
    return
        status == 200
        && "OK".equals(msg)
        && result != null
        && !result.isEmpty();
  }
}
