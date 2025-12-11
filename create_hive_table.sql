-- 创建Hive目标表：存储千问模型生成的标签结果
-- 表名：dwd.dwd_device_cloudstorage_tag_by_qianwen

CREATE DATABASE IF NOT EXISTS dwd;

-- 删除表（如果需要重建）
-- DROP TABLE IF EXISTS dwd.dwd_device_cloudstorage_tag_by_qianwen;

CREATE TABLE IF NOT EXISTS dwd.dwd_device_cloudstorage_tag_by_qianwen (
    dev_serial STRING COMMENT '设备序列号',
    tag_result STRING COMMENT '千问模型生成的标签结果'
)
COMMENT '设备云存储视频标签表（千问模型生成）'
PARTITIONED BY (dt STRING COMMENT '日期分区，格式：yyyy-MM-dd')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'creator'='bigdata-team',
    'created_date'='2025-12-11'
);

-- 验证表创建成功
SHOW TABLES IN dwd LIKE 'dwd_device_cloudstorage_tag_by_qianwen';

-- 查看表结构
DESC FORMATTED dwd.dwd_device_cloudstorage_tag_by_qianwen;
