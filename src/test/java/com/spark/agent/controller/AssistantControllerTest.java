package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.service.AssistantService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AssistantControllerTest {

    @Mock
    private AssistantService assistantService;

    @InjectMocks
    private AssistantController controller;

    @Test
    void ask_returnsAnswerWrappedInR() {
        when(assistantService.answer("2号注塑机最近有什么异常"))
                .thenReturn("2号注塑机最近1小时内触发了1次压力过高告警。");

        R<String> result = controller.ask(new AssistantController.AssistantRequest("2号注塑机最近有什么异常"));

        assertEquals(0, result.getCode());
        assertEquals("success", result.getMsg());
        assertEquals("2号注塑机最近1小时内触发了1次压力过高告警。", result.getData());
    }
}
