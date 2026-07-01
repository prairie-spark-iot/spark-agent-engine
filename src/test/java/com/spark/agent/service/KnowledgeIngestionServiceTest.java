package com.spark.agent.service;

import com.spark.agent.common.SnowflakeIdGenerator;
import com.spark.agent.dto.KnowledgeImportItem;
import com.spark.agent.dto.KnowledgeImportResult;
import com.spark.agent.entity.Knowledge;
import com.spark.agent.repository.KnowledgeRepository;
import com.spark.agent.repository.VectorStoreRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.embedding.EmbeddingModel;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KnowledgeIngestionServiceTest {

    @Mock
    private EmbeddingModel embeddingModel;
    @Mock
    private KnowledgeRepository knowledgeRepository;
    @Mock
    private VectorStoreRepository vectorStoreRepository;
    @Mock
    private SnowflakeIdGenerator idGen;

    private KnowledgeIngestionService service;

    @BeforeEach
    void setUp() {
        service = new KnowledgeIngestionService(embeddingModel, knowledgeRepository,
                vectorStoreRepository, idGen);
    }

    // ---- chunk() tests via ingest ----

    @Test
    void ingest_shortText_createsSingleChunk() {
        String text = "Short manual text.";
        when(embeddingModel.embed(anyString())).thenReturn(new float[]{0.1f, 0.2f});
        when(idGen.nextId()).thenReturn(1L);

        int chunks = service.ingest("test", text, (short) 1, "model1", 1L, "source1");

        assertEquals(1, chunks);
        verify(knowledgeRepository).save(any(Knowledge.class));
        verify(vectorStoreRepository).saveEmbedding(eq(1L), any(float[].class));
    }

    @Test
    void ingest_multipleParagraphs_mergedIntoOneChunkWhenSmall() {
        // Small paragraphs (< 500 chars each) are merged into a single chunk
        String text = "Paragraph one.\n\nParagraph two.\n\nParagraph three.";
        when(embeddingModel.embed(anyString())).thenReturn(new float[]{0.1f, 0.2f});
        when(idGen.nextId()).thenReturn(1L);

        int chunks = service.ingest("test", text, (short) 1, null, null, "test");

        assertEquals(1, chunks);
        verify(vectorStoreRepository).saveEmbedding(anyLong(), any(float[].class));
    }

    @Test
    void ingest_largeParagraph_splitByChunkSize() {
        // 1500 chars with no paragraph breaks → split every 500
        String text = "a".repeat(1500);
        when(embeddingModel.embed(anyString())).thenReturn(new float[]{0.1f, 0.2f});
        when(idGen.nextId()).thenReturn(1L, 2L, 3L);

        int chunks = service.ingest("test", text, (short) 1, null, null, "test");

        assertEquals(3, chunks);
    }

    @Test
    void ingest_emptyText_createsSingleEntry() {
        String text = "";
        when(embeddingModel.embed(anyString())).thenReturn(new float[]{0.1f});
        when(idGen.nextId()).thenReturn(1L);

        int chunks = service.ingest("test", text, (short) 1, null, null, "test");

        assertEquals(1, chunks);
    }

    @Test
    void ingest_embeddingFailure_doesNotSaveChunks() {
        String text = "Some text.";
        when(embeddingModel.embed(anyString())).thenThrow(new RuntimeException("Ollama timed out"));

        assertThrows(RuntimeException.class,
                () -> service.ingest("test", text, (short) 1, null, null, "test"));

        verifyNoInteractions(knowledgeRepository);
        verifyNoInteractions(vectorStoreRepository);
    }

    // ---- importBatch tests ----

    @Test
    void importBatch_allSucceed() {
        List<KnowledgeImportItem> items = List.of(
                new KnowledgeImportItem("title1", 1L, "model1", (short) 1, "src1", "chunk1"),
                new KnowledgeImportItem("title2", 1L, "model1", (short) 1, "src1", "chunk2")
        );
        when(embeddingModel.embed(anyString())).thenReturn(new float[]{0.1f, 0.2f});
        when(idGen.nextId()).thenReturn(1L, 2L);

        KnowledgeImportResult result = service.importBatch(items);

        assertEquals(2, result.successCount());
        assertEquals(0, result.failCount());
        assertTrue(result.failedItems().isEmpty());
    }

    @Test
    void importBatch_partialFailures() {
        List<KnowledgeImportItem> items = List.of(
                new KnowledgeImportItem("good", 1L, "model1", (short) 1, "src1", "good text"),
                new KnowledgeImportItem("bad", 1L, "model1", (short) 1, "src1", "bad text")
        );
        when(embeddingModel.embed("good text")).thenReturn(new float[]{0.1f});
        when(embeddingModel.embed("bad text")).thenThrow(new RuntimeException("embed failed"));
        when(idGen.nextId()).thenReturn(1L);

        KnowledgeImportResult result = service.importBatch(items);

        assertEquals(1, result.successCount());
        assertEquals(1, result.failCount());
        assertEquals("bad", result.failedItems().get(0).title());
    }

    @Test
    void importBatch_allFailures() {
        List<KnowledgeImportItem> items = List.of(
                new KnowledgeImportItem("fail1", 1L, "model1", (short) 1, "src1", "text1"),
                new KnowledgeImportItem("fail2", 1L, "model1", (short) 1, "src1", "text2")
        );
        when(embeddingModel.embed(anyString())).thenThrow(new RuntimeException("Ollama unavailable"));

        KnowledgeImportResult result = service.importBatch(items);

        assertEquals(0, result.successCount());
        assertEquals(2, result.failCount());
    }

    @Test
    void importBatch_emptyList() {
        KnowledgeImportResult result = service.importBatch(List.of());
        assertEquals(0, result.successCount());
        assertEquals(0, result.failCount());
    }

    // ---- saveChunks transactional boundary test ----

    @Test
    void saveChunks_savesKnowledgeAndEmbeddingInOrder() {
        when(idGen.nextId()).thenReturn(1L, 2L);

        service.saveChunks("title", (short) 1, "model1", 1L, "src1",
                List.of("chunk1", "chunk2"),
                List.of(new float[]{0.1f}, new float[]{0.2f}));

        ArgumentCaptor<Knowledge> knowledgeCaptor = ArgumentCaptor.forClass(Knowledge.class);
        verify(knowledgeRepository, times(2)).save(knowledgeCaptor.capture());
        assertEquals("chunk1", knowledgeCaptor.getAllValues().get(0).getChunkText());
        assertEquals("chunk2", knowledgeCaptor.getAllValues().get(1).getChunkText());

        verify(vectorStoreRepository).saveEmbedding(1L, new float[]{0.1f});
        verify(vectorStoreRepository).saveEmbedding(2L, new float[]{0.2f});
    }
}
