# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Stack

- **Java 25** + **Spring Boot 4.1.0** (Gradle wrapper `./gradlew`)
- Package root: `com.spark.agent`

## Commands

```bash
# Build
./gradlew build

# Run
./gradlew bootRun

# Test (all)
./gradlew test

# Single test class
./gradlew test --tests "com.spark.agent.SparkAgentEngineApplicationTests"

# Assemble JAR without tests
./gradlew assemble
```

## Project State

This is a greenfield scaffold — only the `@SpringBootApplication` entry point and a context-load test exist. All features are yet to be built.

Source lives under `src/main/java/com/spark/agent/`; config in `src/main/resources/application.yaml`.
