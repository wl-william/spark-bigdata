# Spark千问视频文本处理器

基于Spark的大数据处理应用，使用阿里云千问大模型对视频文本进行智能标签生成。

## 功能特性

- ✅ 从Hive表读取视频文本数据
- ✅ 调用千问大模型生成智能标签
- ✅ 实现TPM（Tokens Per Minute）限流控制
- ✅ 自动重试机制，处理临时错误
- ✅ 分区并行处理，提高效率
- ✅ 结果写回Hive表
- ✅ 完善的日志和错误处理

## 技术栈

- **Java**: 1.8
- **Spark**: 2.4.8 (Scala 2.11)
- **千问模型**: qwen3-235b-a22b-instruct-2507
- **构建工具**: Maven 3.x

## 项目结构

```
spark-qianwen-processor/
├── src/main/java/com/bigdata/spark/
│   ├── QianwenVideoTextProcessor.java    # 主程序
│   ├── service/
│   │   └── QianwenService.java           # 千问API调用服务
│   ├── util/
│   │   └── RateLimiter.java              # 限流器
│   └── model/
│       ├── VideoTextRecord.java          # 视频文本记录
│       └── TaggedRecord.java             # 标签结果记录
├── src/main/resources/
│   ├── log4j.properties                  # 日志配置
│   └── application.properties            # 应用配置
├── pom.xml                               # Maven配置
├── create_hive_table.sql                 # Hive表DDL
└── run.sh                                # 运行脚本
```

## 前置条件

### 1. 环境要求

- JDK 1.8+
- Maven 3.x
- Spark 2.4.x (Scala 2.11)
- Hadoop/Hive集群访问权限

### 2. API密钥

需要阿里云DashScope API Key：

```bash
export DASHSCOPE_API_KEY="sk-your-api-key-here"
```

获取方式：https://dashscope.console.aliyun.com/

### 3. Hive表准备

执行 `create_hive_table.sql` 创建目标表：

```bash
beeline -u "jdbc:hive2://your-hive-server:10000" -f create_hive_table.sql
```

## 快速开始

### 1. 编译打包

```bash
cd spark-qianwen-processor
mvn clean package
```

生成的jar包：`target/spark-qianwen-processor-1.0.0.jar`

### 2. 设置API密钥

```bash
export DASHSCOPE_API_KEY="sk-xxxxxxxxxxxxx"
```

### 3. 运行任务

#### 方式一：使用脚本运行

```bash
./run.sh 2025-12-01
```

#### 方式二：使用spark-submit

```bash
spark-submit \
  --class com.bigdata.spark.QianwenVideoTextProcessor \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 2 \
  --executor-memory 4G \
  --driver-memory 2G \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  target/spark-qianwen-processor-1.0.0.jar \
  2025-12-01
```

## 配置说明

### 限流配置

在 `QianwenVideoTextProcessor.java` 中配置：

```java
private static final int TPM_LIMIT = 100000;  // 每分钟最多100000个token
private static final int BATCH_SIZE = 100;    // 批处理大小
```

### 重试配置

```java
private static final int MAX_RETRIES = 3;      // 最大重试次数
private static final int RETRY_DELAY_MS = 2000; // 重试延迟（毫秒）
```

### Spark配置

可以通过 `spark-submit` 参数调整：

```bash
--num-executors 20           # executor数量（根据数据量调整）
--executor-cores 2           # 每个executor的核心数
--executor-memory 4G         # executor内存
--driver-memory 2G           # driver内存
```

## 监控和日志

### 查看Spark应用状态

```bash
# YARN
yarn application -list

# Spark History Server
http://your-spark-history-server:18080
```

### 查看日志

```bash
# Driver日志
yarn logs -applicationId application_xxx_xxx

# 实时日志（如果是client模式）
tail -f /path/to/spark/logs/*
```

## 性能优化建议

### 1. 分区数调整

根据数据量调整executor数量：

```
executor数量 ≈ 数据量(GB) / 2
```

### 2. 限流调整

如果有多个Spark executor同时运行，需要分配TPM：

```
每个分区的TPM = 总TPM / executor数量
```

### 3. 批处理大小

根据文本长度调整批处理大小：
- 短文本（<500字）：BATCH_SIZE = 100-200
- 长文本（>1000字）：BATCH_SIZE = 20-50

## 故障排查

### 1. API密钥错误

**错误**: `NoApiKeyException`

**解决**:
```bash
export DASHSCOPE_API_KEY="your-key"
echo $DASHSCOPE_API_KEY  # 验证
```

### 2. 速率限制

**错误**: `rate limit exceeded`

**解决**:
- 降低TPM_LIMIT值
- 减少并发executor数量
- 增加重试延迟时间

### 3. 内存不足

**错误**: `OutOfMemoryError`

**解决**:
```bash
--executor-memory 8G
--driver-memory 4G
```

### 4. Hive表不存在

**错误**: `Table not found`

**解决**:
```bash
# 创建表
beeline -f create_hive_table.sql

# 验证表存在
hive -e "SHOW TABLES IN dwd LIKE 'dwd_device_cloudstorage_tag_by_qianwen'"
```

## 数据流程

```
1. 读取Hive源表
   ↓
2. 过滤空值
   ↓
3. 分区并行处理
   ↓ (每个分区)
4. 限流器控制
   ↓
5. 调用千问API
   ↓
6. 错误重试
   ↓
7. 收集结果
   ↓
8. 写入Hive目标表
```

## 成本估算

### API调用成本

千问模型按token计费，假设：
- 平均每条文本：1000 tokens（输入+输出）
- 处理10万条记录：1亿tokens
- 成本：根据阿里云定价计算

### 计算资源成本

- Executor数量：10个
- 运行时间：约2-4小时（10万条）
- 成本：根据云平台定价

## 开发和测试

### 本地测试

```bash
# 设置本地Spark
export SPARK_HOME=/path/to/spark
export HADOOP_HOME=/path/to/hadoop

# 本地模式运行（小数据集）
spark-submit \
  --class com.bigdata.spark.QianwenVideoTextProcessor \
  --master local[*] \
  target/spark-qianwen-processor-1.0.0.jar \
  2025-12-01
```

### 单元测试

```bash
mvn test
```

## 版本历史

- **v1.0.0** (2025-12-11)
  - 初始版本
  - 支持基本的视频文本标签生成
  - 实现TPM限流和重试机制

## 许可证

内部项目

## 联系方式

如有问题，请联系大数据团队。
