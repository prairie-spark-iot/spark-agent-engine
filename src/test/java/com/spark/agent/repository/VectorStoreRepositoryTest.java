package com.spark.agent.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class VectorStoreRepositoryTest {

    @Mock private JdbcTemplate jdbc;
    @Mock private PreparedStatement ps;

    private VectorStoreRepository repository;

    @BeforeEach
    void setUp() {
        repository = new VectorStoreRepository(jdbc);
    }

    @Test
    void insertKnowledgeBatch_issuesSingleBatchUpdateCallForAllRows() throws SQLException {
        List<VectorStoreRepository.KnowledgeBatchRow> rows = List.of(
                new VectorStoreRepository.KnowledgeBatchRow(1L, "title", (short) 1, 10L, "model1", "src1",
                        "chunk1", new float[]{0.1f, 0.2f}),
                new VectorStoreRepository.KnowledgeBatchRow(2L, "title", (short) 1, 10L, "model1", "src1",
                        "chunk2", new float[]{0.3f, 0.4f})
        );

        repository.insertKnowledgeBatch(rows);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ParameterizedPreparedStatementSetter<VectorStoreRepository.KnowledgeBatchRow>> pssCaptor =
                (ArgumentCaptor<ParameterizedPreparedStatementSetter<VectorStoreRepository.KnowledgeBatchRow>>)
                        (ArgumentCaptor<?>) ArgumentCaptor.forClass(ParameterizedPreparedStatementSetter.class);
        verify(jdbc, times(1)).batchUpdate(sqlCaptor.capture(), eq(rows), eq(rows.size()), pssCaptor.capture());
        assertEquals(1, sqlCaptor.getAllValues().size());
        assertEquals(true, sqlCaptor.getValue().contains("INSERT INTO aiot_knowledge"));

        pssCaptor.getValue().setValues(ps, rows.get(0));
        verify(ps).setLong(1, 1L);
        verify(ps).setString(2, "title");
        verify(ps).setString(6, "chunk1");
        verify(ps).setString(7, "[0.1,0.2]");
        verify(ps).setString(8, "src1");
    }

    @Test
    void insertKnowledgeBatch_emptyList_doesNotCallJdbc() {
        repository.insertKnowledgeBatch(List.of());

        verifyNoInteractions(jdbc);
    }
}
