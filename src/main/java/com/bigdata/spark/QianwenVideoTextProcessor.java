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
    private static final int TPM_LIMIT = 100000; // Tokens per minute
    private static final int BATCH_SIZE = 100; // 每批处理的记录数
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 2000;

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

        // 2. 转换为JavaRDD进行处理
        Dataset<Row> resultDataset = sourceData.mapPartitions(
            (Iterator<Row> partition) -> {
                List<Row> results = new ArrayList<>();

                // 为每个分区创建限流器和服务实例（避免序列化问题）
                RateLimiter rateLimiter = new RateLimiter(TPM_LIMIT);
                QianwenService qianwenService = new QianwenService(
                    API_KEY,
                    MODEL_NAME,
                    MAX_RETRIES,
                    RETRY_DELAY_MS
                );

                int processedCount = 0;
                int errorCount = 0;

                while (partition.hasNext()) {
                    Row row = partition.next();
                    String devSerial = row.getString(0);
                    String videoTextMerged = row.getString(1);

                    try {
                        // 限流控制
                        int estimatedTokens = estimateTokens(videoTextMerged);
                        rateLimiter.acquire(estimatedTokens);

                        // 调用千问模型
                        String tag = qianwenService.generateTag(videoTextMerged);

                        // 创建结果行
                        results.add(org.apache.spark.sql.RowFactory.create(devSerial, tag));
                        processedCount++;

                        if (processedCount % 10 == 0) {
                            logger.info("当前分区已处理 {} 条记录", processedCount);
                        }

                    } catch (Exception e) {
                        errorCount++;
                        logger.error("处理记录失败 dev_serial={}: {}", devSerial, e.getMessage());
                        // 记录失败但继续处理
                        results.add(org.apache.spark.sql.RowFactory.create(devSerial, "ERROR: " + e.getMessage()));
                    }
                }

                logger.info("分区处理完成: 成功={}, 失败={}", processedCount, errorCount);
                return results.iterator();
            },
            RowEncoder.apply(
                new StructType()
                    .add("dev_serial", DataTypes.StringType, false)
                    .add("tag_result", DataTypes.StringType, false)
            )
        );

        // 3. 添加分区字段
        Dataset<Row> finalDataset = resultDataset.withColumn("dt",
            org.apache.spark.sql.functions.lit(processDate));

        // 4. 优化输出文件数
        // 获取数据量并计算合适的分区数
        long totalCount = finalDataset.count();
        logger.info("准备写入 {} 条记录", totalCount);

        // 根据数据量动态调整分区数（每个分区约10万条记录）
        int targetPartitions = Math.max(1, (int) Math.ceil(totalCount / 100000.0));
        // 限制最大分区数为50，避免产生过多小文件
        targetPartitions = Math.min(targetPartitions, 50);

        logger.info("使用 {} 个分区写入数据", targetPartitions);

        // 重分区以控制输出文件数
        Dataset<Row> optimizedDataset = finalDataset.coalesce(targetPartitions);

        // 5. 写入Hive表
        String targetTable = "dwd.dwd_device_cloudstorage_tag_by_qianwen";
        logger.info("写入目标表: {}", targetTable);

        optimizedDataset.write()
            .mode(SaveMode.Append)
            .partitionBy("dt")
            .format("hive")
            .saveAsTable(targetTable);

        logger.info("成功写入 {} 条记录到表 {}，预计生成 {} 个文件", totalCount, targetTable, targetPartitions);
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
