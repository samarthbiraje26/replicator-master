package com.aniket.mirror.replicator.entity;

import com.aniket.mirror.events.FileUploadEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

@Entity
@Table(
    name = "files",
    uniqueConstraints = {
        @UniqueConstraint(name = "uq_files_s3", columnNames = {"s3_bucket", "s3_key"})
    },
    indexes = {
        @Index(name = "idx_files_created_at", columnList = "source_created_at")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileRecord {

  @Id
  @Column(name = "file_id", nullable = false, updatable = false)
  private String fileId;

  @Column(name = "file_name", nullable = false)
  private String fileName;

  @Column(name = "content_type")
  private String contentType;

  @Column(name = "size_bytes", nullable = false)
  private long sizeBytes;

  @Column(name = "s3_bucket", nullable = false)
  private String s3Bucket;

  @Column(name = "s3_key", nullable = false)
  private String s3Key;

  @Column(name = "s3_url", columnDefinition = "TEXT")
  private String s3Url;

  @Column(name = "checksum")
  private String checksum;

  /** Timestamp carried from the event (source-of-truth time). */
  @Column(name = "source_created_at")
  private Instant sourceCreatedAt;

  @CreationTimestamp
  @Column(name = "created_at", updatable = false)
  private Instant createdAt;

  @UpdateTimestamp
  @Column(name = "updated_at")
  private Instant updatedAt;

  public static FileRecord fromEvent(FileUploadEvent event) {
    FileRecord record = new FileRecord();
    record.setFileId(event.getFileId());
    record.setFileName(event.getFileName());
    record.setContentType(event.getContentType());
    record.setSizeBytes(event.getSizeBytes());
    record.setS3Bucket(event.getS3Bucket());
    record.setS3Key(event.getS3Key());
    record.setS3Url(event.getS3Url());
    record.setChecksum(event.getChecksum());
    record.setSourceCreatedAt(event.getCreatedAt());
    return record;
  }
}
