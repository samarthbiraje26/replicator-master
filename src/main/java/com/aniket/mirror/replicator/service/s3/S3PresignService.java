package com.aniket.mirror.replicator.service.s3;

import com.aniket.mirror.replicator.entity.FileReplicationJob;
import java.time.temporal.ChronoUnit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URL;
import java.time.Duration;

@Service
@RequiredArgsConstructor
public class S3PresignService {

  private final S3Presigner presigner;


  public String generateS3Url(FileReplicationJob job){
    String bucket = job.getFile().getS3Bucket();
    String key = job.getFile().getS3Key();
    Duration expiry = Duration.of(10, ChronoUnit.MINUTES);
    URL preSignedURL = createPresignedDownloadUrl(bucket, key, expiry);
    return preSignedURL.toString();
  }

  private URL createPresignedDownloadUrl(String bucket, String key, Duration expiry) {

    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();

    GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
        .signatureDuration(expiry) // e.g. 10 minutes
        .getObjectRequest(getObjectRequest)
        .build();

    return presigner.presignGetObject(presignRequest).url();
  }
}
