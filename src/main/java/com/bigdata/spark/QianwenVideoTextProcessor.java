package com.bigdata.spark;

import com.bigdata.spark.model.VideoTextRecord;
import com.bigdata.spark.model.TaggedRecord;
import com.bigdata.spark.service.QianwenService;
import com.bigdata.spark.util.RateLimiter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spark应用：从Hive读取视频文本数据，调用千问模型生成标签，写回Hive
 *
 * 功能特性：
 * 1. 从Hive表读取数据
 * 2. 调用千问大模型进行文本分析
 * 3. 实现TPM限流控制
 * 4. 容错重试机制
 * 5. 批量处理优化
 */
public class QianwenVideoTextProcessor implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(QianwenVideoTextProcessor.class);

    // 配置参数
    private static final String API_KEY = System.getenv("DASHSCOPE_API_KEY");
    private static final String MODEL_NAME = "qwen3-235b-a22b-instruct-2507";
    private static final int TPM_LIMIT = 100000; // Tokens per minute (账户级别总配额)
    private static final int BATCH_SIZE = 100; // 每批处理的记录数
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000; // 降低重试延迟以加快恢复

    // 并行度配置：根据API限流和集群资源合理设置
    private static final int PROCESS_PARTITIONS = 10; // 处理分区数

    // 【P0优化】分区内并发配置 - 这是提升吞吐量的关键
    // 每个分区内部使用多线程并发调用API，大幅提升吞吐量
    private static final int CONCURRENT_CALLS_PER_PARTITION = 5; // 每分区并发调用数

    // RPM限制（请求数/分钟）- 通常API有独立的RPM限制
    // 千问API RPM限制通常较高（如1000-3000 RPM），这里设保守值
    private static final int RPM_LIMIT = 500; // 总请求数/分钟限制

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: QianwenVideoTextProcessor <date>");
            System.err.println("Example: QianwenVideoTextProcessor 2025-12-01");
            System.exit(1);
        }

        String processDate = args[0];
        logger.info("开始处理日期: {}", processDate);

        // 验证API Key
        if (API_KEY == null || API_KEY.isEmpty()) {
            logger.error("DASHSCOPE_API_KEY环境变量未设置");
            System.exit(1);
        }

        // 创建Spark配置
        SparkConf conf = new SparkConf()
            .setAppName("QianwenVideoTextProcessor-" + processDate)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // 自适应查询执行
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            // Hive动态分区配置
            .set("hive.exec.dynamic.partition", "true")
            .set("hive.exec.dynamic.partition.mode", "nonstrict")
            .set("hive.exec.max.dynamic.partitions", "10000")
            .set("hive.exec.max.dynamic.partitions.pernode", "1000")
            // 控制输出文件数
            .set("spark.sql.shuffle.partitions", "50")  // 减少shuffle分区数
            .set("spark.sql.files.maxRecordsPerFile", "100000");  // 每个文件最多10万条记录

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .config(conf)
            .enableHiveSupport()
            .getOrCreate();

        try {
            processData(spark, processDate);
            logger.info("数据处理完成");
        } catch (Exception e) {
            logger.error("处理失败", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * 处理数据主流程
     */
    private static void processData(SparkSession spark, String processDate) {
        // 1. 从Hive读取源数据
        String sourceSQL = String.format(
            "SELECT dev_serial, video_text_merged " +
            "FROM dwd.hik_cloud2_video_text_result_hist_rand10_grouped_data_merged " +
            "WHERE dt = '%s'",
            processDate
        );

        logger.info("执行SQL: {}", sourceSQL);
        Dataset<Row> sourceData = spark.sql(sourceSQL);

        // 过滤空值
        sourceData = sourceData.filter("dev_serial IS NOT NULL AND video_text_merged IS NOT NULL");

        long totalRecords = sourceData.count();
        logger.info("读取到 {} 条记录", totalRecords);

        if (totalRecords == 0) {
            logger.warn("没有数据需要处理");
            return;
        }

        // 2. 【P0优化】重分区以提升并行度
        // 使用repartition而非coalesce，确保数据均匀分布到指定分区数
        int sourcePartitions = sourceData.rdd().getNumPartitions();
        logger.info("源数据分区数: {}, 将重分区为: {}", sourcePartitions, PROCESS_PARTITIONS);

        Dataset<Row> repartitionedData = sourceData.repartition(PROCESS_PARTITIONS);

        // 【P0优化】计算每个分区的限流配额
        // 将总配额平分到各分区，避免超出账户级别TPM限制
        final int perPartitionTPM = TPM_LIMIT / PROCESS_PARTITIONS;
        final int perPartitionRPM = RPM_LIMIT / PROCESS_PARTITIONS;
        logger.info("===== 限流配置 =====");
        logger.info("每分区TPM配额: {} (总配额: {})", perPartitionTPM, TPM_LIMIT);
        logger.info("每分区RPM配额: {} (总配额: {})", perPartitionRPM, RPM_LIMIT);
        logger.info("每分区并发数: {}", CONCURRENT_CALLS_PER_PARTITION);
        logger.info("总并发能力: {} 个并发请求", PROCESS_PARTITIONS * CONCURRENT_CALLS_PER_PARTITION);

        // 3. 转换为JavaRDD进行处理（使用并发调用）
        Dataset<Row> resultDataset = repartitionedData.mapPartitions(
            (Iterator<Row> partition) -> {
                List<Row> results = new ArrayList<>();

                // 为每个分区创建限流器
                RateLimiter tpmLimiter = new RateLimiter(perPartitionTPM);
                RateLimiter rpmLimiter = new RateLimiter(perPartitionRPM); // 请求数限流

                // 【P0优化】创建线程池用于并发API调用
                ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CALLS_PER_PARTITION);
                List<CompletableFuture<Row>> futures = new ArrayList<>();

                // 性能统计
                AtomicInteger processedCount = new AtomicInteger(0);
                AtomicInteger errorCount = new AtomicInteger(0);
                AtomicLong totalApiTimeMs = new AtomicLong(0);
                AtomicLong totalWaitTimeMs = new AtomicLong(0);
                long partitionStartTime = System.currentTimeMillis();

                // 收集所有待处理的记录
                List<Row> pendingRows = new ArrayList<>();
                while (partition.hasNext()) {
                    pendingRows.add(partition.next());
                }
                int totalInPartition = pendingRows.size();
                logger.info("分区开始处理，共 {} 条记录，并发数 {}", totalInPartition, CONCURRENT_CALLS_PER_PARTITION);

                // 为每条记录提交异步任务
                for (Row row : pendingRows) {
                    String devSerial = row.getString(0);
                    String videoTextMerged = row.getString(1);

                    CompletableFuture<Row> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            // 限流控制（TPM和RPM双重限流）
                            long waitStart = System.currentTimeMillis();
                            int estimatedTokens = estimateTokens(videoTextMerged);

                            synchronized (tpmLimiter) {
                                tpmLimiter.acquire(estimatedTokens);
                            }
                            synchronized (rpmLimiter) {
                                rpmLimiter.acquire(1); // 每个请求计1
                            }

                            long waitEnd = System.currentTimeMillis();
                            totalWaitTimeMs.addAndGet(waitEnd - waitStart);

                            // 【P0优化】每个线程独立创建QianwenService，避免共享状态问题
                            QianwenService qianwenService = new QianwenService(
                                API_KEY,
                                MODEL_NAME,
                                MAX_RETRIES,
                                RETRY_DELAY_MS
                            );

                            // 调用千问模型并计时
                            long apiStart = System.currentTimeMillis();
                            String tag = qianwenService.generateTag(videoTextMerged);
                            long apiEnd = System.currentTimeMillis();

                            totalApiTimeMs.addAndGet(apiEnd - apiStart);
                            int count = processedCount.incrementAndGet();

                            // 每处理20条输出一次进度和性能指标
                            if (count % 20 == 0) {
                                long elapsed = System.currentTimeMillis() - partitionStartTime;
                                double tps = count * 1000.0 / elapsed;
                                double avgApi = totalApiTimeMs.get() * 1.0 / count;
                                double avgWait = totalWaitTimeMs.get() * 1.0 / count;
                                logger.info("[性能] 进度: {}/{}, 吞吐量: {}条/秒, 平均API耗时: {}ms, 平均等待: {}ms",
                                    count, totalInPartition, String.format("%.2f", tps),
                                    String.format("%.0f", avgApi), String.format("%.0f", avgWait));
                            }

                            return org.apache.spark.sql.RowFactory.create(devSerial, tag);

                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            logger.error("处理记录失败 dev_serial={}: {}", devSerial, e.getMessage());
                            return org.apache.spark.sql.RowFactory.create(devSerial, "ERROR: " + e.getMessage());
                        }
                    }, executor);

                    futures.add(future);
                }

                // 等待所有任务完成
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                // 收集结果
                for (CompletableFuture<Row> future : futures) {
                    try {
                        results.add(future.get());
                    } catch (Exception e) {
                        logger.error("获取结果失败: {}", e.getMessage());
                    }
                }

                // 关闭线程池
                executor.shutdown();

                // 输出分区处理统计
                long partitionEndTime = System.currentTimeMillis();
                long totalTime = partitionEndTime - partitionStartTime;
                int processed = processedCount.get();
                int errors = errorCount.get();
                double throughput = processed * 1000.0 / totalTime;
                double avgApiTime = processed > 0 ? totalApiTimeMs.get() * 1.0 / processed : 0;
                double avgWaitTime = processed > 0 ? totalWaitTimeMs.get() * 1.0 / processed : 0;

                logger.info("===== 分区处理完成 =====");
                logger.info("处理结果: 成功={}, 失败={}, 总记录={}", processed, errors, totalInPartition);
                logger.info("耗时统计: 总耗时={}秒, 吞吐量={}条/秒", totalTime / 1000, String.format("%.2f", throughput));
                logger.info("延迟分析: 平均API耗时={}ms, 平均限流等待={}ms", String.format("%.0f", avgApiTime), String.format("%.0f", avgWaitTime));
                if (avgWaitTime > avgApiTime) {
                    logger.warn("【瓶颈诊断】限流等待时间({})ms > API调用时间({}ms)，瓶颈是限流配置，建议提高TPM/RPM配额",
                        String.format("%.0f", avgWaitTime), String.format("%.0f", avgApiTime));
                } else {
                    logger.info("【瓶颈诊断】API调用时间({}ms) >= 限流等待({}ms)，瓶颈是API响应速度，当前并发配置合理",
                        String.format("%.0f", avgApiTime), String.format("%.0f", avgWaitTime));
                }

                return results.iterator();
            },
            RowEncoder.apply(
                new StructType()
                    .add("dev_serial", DataTypes.StringType, false)
                    .add("tag_result", DataTypes.StringType, false)
            )
        );

        // 4. 添加分区字段
        Dataset<Row> finalDataset = resultDataset.withColumn("dt",
            org.apache.spark.sql.functions.lit(processDate));

        // 5. 优化输出文件数
        // 【重要修复】根据源数据量估算分区数，避免在API调用后再count导致重复执行
        // totalRecords 已在前面获取，直接使用
        int targetPartitions = Math.max(1, (int) Math.ceil(totalRecords / 100000.0));
        // 限制最大分区数为50，避免产生过多小文件
        targetPartitions = Math.min(targetPartitions, 50);

        logger.info("预计处理 {} 条记录，使用 {} 个分区写入数据", totalRecords, targetPartitions);

        // 重分区以控制输出文件数
        Dataset<Row> optimizedDataset = finalDataset.coalesce(targetPartitions);

        // 6. 写入Hive表
        String targetTable = "dwd.dwd_device_cloudstorage_tag_by_qianwen";
        logger.info("写入目标表: {}", targetTable);

        optimizedDataset.write()
            .mode(SaveMode.Append)
            .partitionBy("dt")
            .format("hive")
            .saveAsTable(targetTable);

        logger.info("成功写入数据到表 {}，预计生成 {} 个文件", targetTable, targetPartitions);
    }

    /**
     * 估算文本的token数量（粗略估算：中文1字≈1.5token，英文1词≈1token）
     */
    private static int estimateTokens(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        // 简单估算：平均每个字符约1.5个token
        return (int) (text.length() * 1.5);
    }
}
