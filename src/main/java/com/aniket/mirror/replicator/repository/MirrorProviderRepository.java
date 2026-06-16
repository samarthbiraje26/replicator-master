package com.aniket.mirror.replicator.repository;

import com.aniket.mirror.replicator.constants.FileStatus;
import com.aniket.mirror.replicator.constants.ProviderType;
import com.aniket.mirror.replicator.entity.MirrorProvider;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MirrorProviderRepository extends JpaRepository<MirrorProvider, Long> {



  @Query(value = "SELECT * FROM mirror_provider mp WHERE mp.file_status = :status AND mp.next_poll_at <= :now ORDER BY mp.next_poll_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
  List<MirrorProvider> findPollingCandidates(@Param("status") String status, @Param("now") Instant now, @Param("limit") int limit);

  @Query(value = "SELECT * FROM mirror_provider mp WHERE (mp.last_polled_at < :threshold OR (mp.last_polled_at IS NULL AND mp.created_at < :threshold)) LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
  List<MirrorProvider> findRecheckCandidates(@Param("threshold") Instant threshold, @Param("limit") int limit);

  Optional<MirrorProvider> findByFileReplicationJob_File_FileIdAndProviderName(String fileId, ProviderType providerName);
}
