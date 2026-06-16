package com.aniket.mirror.replicator.dto.response.streamtape.poll;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class STVideoData {
  private String id;
  private String remoteurl;
  private String status;
  private Long bytes_loaded;
  private Long bytes_total;
  private String folderid;
  private String added;
  private String last_update;
  private Object linkid;
  private Object url;
}
