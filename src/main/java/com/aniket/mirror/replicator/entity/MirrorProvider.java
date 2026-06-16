package com.aniket.mirror.replicator.entity;

import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.constants.ProviderType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Version;
import java.time.Instant;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Data
public class MirrorProvider {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @ManyToOne
  @JoinColumn(name = "event_id", nullable = false)
  private FileReplicationJob fileReplicationJob;

  @Enumerated(EnumType.STRING)
  private ProviderType providerName;

  @Enumerated(EnumType.STRING)
  private FileStatus fileStatus;

  private String remoteUploadId;

  private String externalFileId;

  // ---------- Polling related fields ----------

  private Instant lastPolledAt;

  private Instant nextPollAt;

  private int pollAttemptCount;

  private String lastError;

  @Version
  private Long version;

//  --------------------------------------------


  @CreationTimestamp
  private Instant createdAt;

  @UpdateTimestamp
  private Instant updatedAt;

}
