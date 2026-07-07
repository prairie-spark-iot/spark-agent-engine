package com.spark.agent.config;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * Spring AI's OllamaApiAutoConfiguration builds its RestClient from whichever
 * RestClient.Builder bean is present, falling back to an untimed RestClient.builder()
 * otherwise. Without this, a slow/hung Ollama call is only abandoned at the
 * CompletableFuture.orTimeout() level in DiagnosisAgentService - that only stops Java
 * from waiting, it does not close the underlying socket, so the request keeps occupying
 * Ollama's single (-np 1, see application.yaml) inference slot and the next diagnosis
 * attempt queues up behind a call nobody is listening to anymore. A real read timeout
 * closes the connection when time is up instead.
 */
@Configuration
@RequiredArgsConstructor
public class OllamaHttpClientConfig {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final AppProperties appProperties;

    @Bean
    public RestClient.Builder restClientBuilder() {
        ClientHttpRequestFactory requestFactory = ClientHttpRequestFactoryBuilder.detect()
                .build(HttpClientSettings.defaults()
                        .withConnectTimeout(CONNECT_TIMEOUT)
                        .withReadTimeout(Duration.ofSeconds(appProperties.getDiagnosisTimeoutSeconds())));
        return RestClient.builder().requestFactory(requestFactory);
    }
}
