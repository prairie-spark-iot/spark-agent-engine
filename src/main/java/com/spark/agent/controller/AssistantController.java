package com.spark.agent.controller;

import com.spark.agent.common.R;
import com.spark.agent.service.AssistantService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/assistant")
@RequiredArgsConstructor
public class AssistantController {

    private final AssistantService assistantService;

    public record AssistantRequest(@NotBlank String question) {}

    @PostMapping
    public R<String> ask(@RequestBody @Valid AssistantRequest req) {
        return R.ok(assistantService.answer(req.question()));
    }
}
