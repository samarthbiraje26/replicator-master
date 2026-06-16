package com.aniket.mirror.replicator.repository;

import com.aniket.mirror.replicator.entity.FileReplicationJob;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FileReplicationJobRepository extends JpaRepository<FileReplicationJob, String> {

  boolean existsByFile_FileId(String fileId);

}
