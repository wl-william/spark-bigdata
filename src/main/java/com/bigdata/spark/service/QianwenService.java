package com.bigdata.spark.service;

import com.alibaba.dashscope.aigc.generation.Generation;
import com.alibaba.dashscope.aigc.generation.GenerationParam;
import com.alibaba.dashscope.aigc.generation.GenerationResult;
import com.alibaba.dashscope.common.Message;
import com.alibaba.dashscope.common.Role;
import com.alibaba.dashscope.exception.ApiException;
import com.alibaba.dashscope.exception.InputRequiredException;
import com.alibaba.dashscope.exception.NoApiKeyException;
import com.alibaba.dashscope.protocol.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 千问模型调用服务
 *
 * 功能：
 * 1. 封装千问API调用
 * 2. 实现重试机制
 * 3. 异常处理
 */
public class QianwenService implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(QianwenService.class);

    private final String apiKey;
    private final String modelName;
    private final int maxRetries;
    private final int retryDelayMs;

    // System prompt - 定义模型的角色和任务
    private static final String SYSTEM_PROMPT =
        "你是一个专业的视频内容分析助手。" +
        "请根据用户提供的视频文本内容，提取关键标签。" +
        "要求：1)标签简洁明了 2)用逗号分隔 3)最多5个标签 4)只返回标签，不要其他说明";

    public QianwenService(String apiKey, String modelName, int maxRetries, int retryDelayMs) {
        this.apiKey = apiKey;
        this.modelName = modelName;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    /**
     * 生成视频文本标签
     *
     * @param videoText 视频文本内容
     * @return 生成的标签字符串
     * @throws Exception 调用失败时抛出异常
     */
    public String generateTag(String videoText) throws Exception {
        if (videoText == null || videoText.trim().isEmpty()) {
            return "空内容";
        }

        Exception lastException = null;

        // 重试机制
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String result = callQianwenAPI(videoText);

                if (result != null && !result.trim().isEmpty()) {
                    return result.trim();
                }

                logger.warn("模型返回空结果，尝试 {}/{}", attempt, maxRetries);

            } catch (ApiException e) {
                lastException = e;

                // 判断是否为可重试的错误
                if (isRetryableError(e)) {
                    logger.warn("API调用失败（可重试），尝试 {}/{}: {}",
                        attempt, maxRetries, e.getMessage());

                    if (attempt < maxRetries) {
                        Thread.sleep(retryDelayMs * attempt); // 指数退避
                    }
                } else {
                    // 不可重试的错误，直接抛出
                    logger.error("API调用失败（不可重试）: {}", e.getMessage());
                    throw e;
                }

            } catch (NoApiKeyException | InputRequiredException e) {
                // 配置错误，不重试
                logger.error("配置错误: {}", e.getMessage());
                throw e;
            } catch (Exception e) {
                lastException = e;
                logger.error("未知错误，尝试 {}/{}: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    Thread.sleep(retryDelayMs * attempt);
                }
            }
        }

        // 所有重试都失败
        throw new Exception("调用千问API失败，已重试" + maxRetries + "次", lastException);
    }

    /**
     * 调用千问API
     */
    private String callQianwenAPI(String videoText)
            throws ApiException, NoApiKeyException, InputRequiredException {

        Generation gen = new Generation(
            Protocol.HTTP.getValue(),
            "https://dashscope.aliyuncs.com/api/v1"
        );

        // 构建消息
        Message systemMsg = Message.builder()
            .role(Role.SYSTEM.getValue())
            .content(SYSTEM_PROMPT)
            .build();

        Message userMsg = Message.builder()
            .role(Role.USER.getValue())
            .content("请分析以下视频文本内容并提取关键标签：\n\n" + videoText)
            .build();

        // 构建请求参数
        GenerationParam param = GenerationParam.builder()
            .apiKey(apiKey)
            .model(modelName)
            .messages(Arrays.asList(systemMsg, userMsg))
            .resultFormat(GenerationParam.ResultFormat.MESSAGE)
            .temperature(0.7f)  // 控制创造性，0-1之间
            .maxTokens(500)     // 限制返回token数
            .topP(0.9f)
            .build();

        // 调用API
        GenerationResult result = gen.call(param);

        // 提取结果
        if (result != null &&
            result.getOutput() != null &&
            result.getOutput().getChoices() != null &&
            !result.getOutput().getChoices().isEmpty()) {

            String content = result.getOutput()
                .getChoices()
                .get(0)
                .getMessage()
                .getContent();

            logger.debug("模型返回: {}", content);
            return content;
        }

        return null;
    }

    /**
     * 判断是否为可重试的错误
     */
    private boolean isRetryableError(ApiException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        // 速率限制、超时、服务器错误等可以重试
        return message.contains("rate limit") ||
               message.contains("timeout") ||
               message.contains("503") ||
               message.contains("502") ||
               message.contains("500") ||
               message.contains("429");
    }
}
