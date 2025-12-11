package com.bigdata.spark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * 基于滑动窗口的令牌桶限流器
 *
 * 功能：
 * 1. 控制每分钟的Token消耗量（TPM）
 * 2. 使用滑动窗口算法
 * 3. 线程安全
 */
public class RateLimiter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(RateLimiter.class);

    private final int tokensPerMinute;
    private final Queue<TokenRecord> tokenUsageWindow;
    private int currentTokenCount;
    private static final long WINDOW_SIZE_MS = 60000L; // 1分钟窗口

    /**
     * Token使用记录
     */
    private static class TokenRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        final long timestamp;
        final int tokens;

        TokenRecord(long timestamp, int tokens) {
            this.timestamp = timestamp;
            this.tokens = tokens;
        }
    }

    public RateLimiter(int tokensPerMinute) {
        this.tokensPerMinute = tokensPerMinute;
        this.tokenUsageWindow = new LinkedList<>();
        this.currentTokenCount = 0;
    }

    /**
     * 申请tokens，如果超过限制则等待
     *
     * @param tokens 需要的token数量
     * @throws InterruptedException 等待被中断
     */
    public synchronized void acquire(int tokens) throws InterruptedException {
        long now = System.currentTimeMillis();

        // 清理过期的记录
        cleanExpiredRecords(now);

        // 检查是否超过限制
        while (currentTokenCount + tokens > tokensPerMinute) {
            // 计算需要等待的时间
            long waitTime = calculateWaitTime(now);

            if (waitTime > 0) {
                logger.debug("限流等待 {}ms，当前使用 {}/{} tokens",
                    waitTime, currentTokenCount, tokensPerMinute);
                Thread.sleep(waitTime);
            }

            now = System.currentTimeMillis();
            cleanExpiredRecords(now);
        }

        // 记录token使用
        tokenUsageWindow.offer(new TokenRecord(now, tokens));
        currentTokenCount += tokens;

        logger.debug("获取 {} tokens，当前窗口使用 {}/{} tokens",
            tokens, currentTokenCount, tokensPerMinute);
    }

    /**
     * 清理过期的token记录（超过1分钟窗口的）
     */
    private void cleanExpiredRecords(long currentTime) {
        long windowStart = currentTime - WINDOW_SIZE_MS;

        while (!tokenUsageWindow.isEmpty()) {
            TokenRecord record = tokenUsageWindow.peek();
            if (record.timestamp < windowStart) {
                tokenUsageWindow.poll();
                currentTokenCount -= record.tokens;
            } else {
                break;
            }
        }
    }

    /**
     * 计算需要等待的时间
     */
    private long calculateWaitTime(long currentTime) {
        if (tokenUsageWindow.isEmpty()) {
            return 0;
        }

        // 找到最早的记录
        TokenRecord oldest = tokenUsageWindow.peek();
        if (oldest == null) {
            return 0;
        }

        // 计算该记录多久后过期
        long windowStart = currentTime - WINDOW_SIZE_MS;
        if (oldest.timestamp < windowStart) {
            return 0;
        }

        // 等待到最早的记录过期
        return oldest.timestamp - windowStart + 100; // 加100ms缓冲
    }

    /**
     * 获取当前窗口内已使用的token数
     */
    public synchronized int getCurrentTokenCount() {
        cleanExpiredRecords(System.currentTimeMillis());
        return currentTokenCount;
    }

    /**
     * 重置限流器
     */
    public synchronized void reset() {
        tokenUsageWindow.clear();
        currentTokenCount = 0;
        logger.info("限流器已重置");
    }
}
