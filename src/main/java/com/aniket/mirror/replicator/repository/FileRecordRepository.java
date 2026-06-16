package com.aniket.mirror.replicator.repository;

import com.aniket.mirror.replicator.entity.FileRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FileRecordRepository extends JpaRepository<FileRecord, String> {
}
