package com.spark.agent.repository;

import com.spark.agent.entity.OutboxMessage;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

    List<OutboxMessage> findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable pageable);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxMessage o SET o.publishedAt = :publishedAt WHERE o.id = :id")
    void markPublished(Long id, LocalDateTime publishedAt);

    @Modifying
    @Transactional
    @Query("DELETE FROM OutboxMessage o WHERE o.publishedAt IS NOT NULL AND o.publishedAt < :cutoff")
    int deletePublishedBefore(LocalDateTime cutoff);
}
