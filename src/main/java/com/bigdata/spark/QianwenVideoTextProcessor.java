package com.bigdata.spark;

import com.bigdata.spark.service.QianwenService;
import com.bigdata.spark.util.RateLimiter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
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
    private static final int TPM_LIMIT = 100000; // Tokens per minute (账户级别总配额)
    private static final int BATCH_SIZE = 100; // 每批处理的记录数
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 2000;

    // 并行度配置：根据API限流和集群资源合理设置
    // 分区数过多会导致限流冲突，过少会浪费并行能力
    private static final int PROCESS_PARTITIONS = 10; // 处理分区数，可根据实际调整

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
     *
     * 【性能优化关键点】:
     * 1. 在API调用后立即缓存结果，防止task失败导致重复调用API
     * 2. 使用insertInto替代saveAsTable提升Hive写入效率
     * 3. 避免不必要的shuffle操作
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

        // 过滤空值并缓存源数据，避免后续操作重复读取Hive
        sourceData = sourceData.filter("dev_serial IS NOT NULL AND video_text_merged IS NOT NULL")
                               .persist(StorageLevel.MEMORY_AND_DISK());

        long totalRecords = sourceData.count();
        logger.info("读取到 {} 条记录", totalRecords);

        if (totalRecords == 0) {
            logger.warn("没有数据需要处理");
            sourceData.unpersist();
            return;
        }

        // 2. 重分区以提升并行度
        int sourcePartitions = sourceData.rdd().getNumPartitions();
        logger.info("源数据分区数: {}, 将重分区为: {}", sourcePartitions, PROCESS_PARTITIONS);

        Dataset<Row> repartitionedData = sourceData.repartition(PROCESS_PARTITIONS);

        // 计算每个分区的限流配额
        final int perPartitionTPM = TPM_LIMIT / PROCESS_PARTITIONS;
        logger.info("每分区TPM配额: {} (总配额: {}，分区数: {})",
            perPartitionTPM, TPM_LIMIT, PROCESS_PARTITIONS);

        // 3. 调用千问API处理数据
        Dataset<Row> resultDataset = repartitionedData.mapPartitions(
            (Iterator<Row> partition) -> {
                List<Row> results = new ArrayList<>();

                RateLimiter rateLimiter = new RateLimiter(perPartitionTPM);
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
                        int estimatedTokens = estimateTokens(videoTextMerged);
                        rateLimiter.acquire(estimatedTokens);

                        String tag = qianwenService.generateTag(videoTextMerged);

                        // 直接创建包含dt字段的完整行，避免后续withColumn操作
                        results.add(org.apache.spark.sql.RowFactory.create(devSerial, tag, processDate));
                        processedCount++;

                        if (processedCount % 10 == 0) {
                            logger.info("当前分区已处理 {} 条记录", processedCount);
                        }

                    } catch (Exception e) {
                        errorCount++;
                        logger.error("处理记录失败 dev_serial={}: {}", devSerial, e.getMessage());
                        results.add(org.apache.spark.sql.RowFactory.create(devSerial, "ERROR: " + e.getMessage(), processDate));
                    }
                }

                logger.info("分区处理完成: 成功={}, 失败={}", processedCount, errorCount);
                return results.iterator();
            },
            RowEncoder.apply(
                new StructType()
                    .add("dev_serial", DataTypes.StringType, false)
                    .add("tag_result", DataTypes.StringType, false)
                    .add("dt", DataTypes.StringType, false)
            )
        );

        // 4. 【关键优化】立即缓存API调用结果到磁盘！
        // 这是防止API被重复调用的核心：确保结果物化后才进行后续操作
        logger.info("正在缓存API调用结果...");
        resultDataset = resultDataset.persist(StorageLevel.MEMORY_AND_DISK());

        // 强制物化数据，触发API调用执行
        long processedRecords = resultDataset.count();
        logger.info("API调用完成，共处理 {} 条记录，结果已缓存", processedRecords);

        // 释放源数据缓存，节省内存
        sourceData.unpersist();

        // 5. 写入Hive表
        // 使用insertInto替代saveAsTable，效率更高
        String targetTable = "dwd.dwd_device_cloudstorage_tag_by_qianwen";
        logger.info("写入目标表: {}", targetTable);

        try {
            // 确保分区存在
            spark.sql(String.format(
                "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (dt='%s')",
                targetTable, processDate
            ));
        } catch (Exception e) {
            logger.warn("创建分区时出现警告（可忽略）: {}", e.getMessage());
        }

        // 直接写入，不使用coalesce避免额外的数据移动
        // AQE会自动合并小分区
        resultDataset.write()
            .mode(SaveMode.Append)
            .format("parquet")
            .insertInto(targetTable);

        logger.info("成功写入数据到表 {}，共 {} 条记录", targetTable, processedRecords);

        // 释放结果缓存
        resultDataset.unpersist();
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
