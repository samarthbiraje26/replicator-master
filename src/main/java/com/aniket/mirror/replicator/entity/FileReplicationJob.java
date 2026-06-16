package com.aniket.mirror.replicator.entity;

import com.aniket.mirror.events.FileUploadEvent;
import com.aniket.mirror.replicator.constants.JobStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import java.time.Instant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileReplicationJob {

  @Id
  private String eventId;

  @ManyToOne(optional = false, cascade = {CascadeType.PERSIST, CascadeType.MERGE})
  @JoinColumn(name = "file_id", nullable = false, unique = true)
  private FileRecord file;

  private JobStatus jobStatus;

  @OneToMany(mappedBy="fileReplicationJob",
      cascade = CascadeType.ALL,
      orphanRemoval = true,
      fetch = FetchType.LAZY)
  private List<MirrorProvider> mirrorProviderList;

  @Version
  private Long version;

  @CreationTimestamp
  private Instant jobCreatedAt;

  @UpdateTimestamp
  private Instant jobUpdatedAt;


  public FileReplicationJob(FileUploadEvent event) {
    this.eventId = event.getEventId();
    this.jobStatus = JobStatus.CREATED;
    this.file = FileRecord.fromEvent(event);
  }


}
