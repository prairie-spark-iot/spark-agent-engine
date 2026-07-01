package com.spark.agent.repository;

import com.spark.agent.entity.Knowledge;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KnowledgeRepository extends JpaRepository<Knowledge, Long> {
}
