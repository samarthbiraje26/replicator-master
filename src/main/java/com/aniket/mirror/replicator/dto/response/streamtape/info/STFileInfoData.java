package com.aniket.mirror.replicator.dto.response.streamtape.info;

import lombok.Data;

@Data
public class STFileInfoData {
    private String id;
    private int status;
    private String name;
    private long size;
    private boolean converted;
}
