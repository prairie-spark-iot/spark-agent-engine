package com.spark.agent.mcp;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class McpToolConfig {

    @Bean
    public ToolCallbackProvider deviceToolCallbacks(DeviceMcpToolService deviceMcpToolService) {
        return MethodToolCallbackProvider.builder()
                .toolObjects(new Object[]{deviceMcpToolService})
                .build();
    }
}
