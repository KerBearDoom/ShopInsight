# ShopInsight

> 基于大数据的电商用户行为分析平台

面向电商**商家经营决策**的数据分析平台：看清流量从哪来、在哪一环流失、哪些商品有吸引力。

技术上是一条完整的 Lambda 链路：埋点数据经 Kafka 承接 → Flink 实时清洗与计算 → ClickHouse 支撑多维分析 → Spark 离线计算经营指标 → Spring Boot 提供 REST API → 移动端展示。

---

## 当前进度

| 阶段 | 内容 | 状态 |
|------|------|------|
| 0 | 环境搭建（Docker / Maven / JDK） | ✅ 完成 |
| 1 | 表结构设计 | ✅ 完成 |
| 1 | 多模块骨架拆分 | ⬜ 未开始 |
| 2 | Kafka → Flink → ClickHouse 链路 | ✅ **已跑通** |
| 2 | 全量数据导入（1 亿行） | ⬜ 未开始 |
| 2 | Checkpoint / exactly-once | ⬜ 未开始 |
| 3 | 实时窗口计算 + REST API | ⬜ 未开始 |
| 4 | Spark 离线计算（漏斗 / 留存 / 分群） | ⬜ 未开始 |
| 5 | 移动端展示 | ⬜ 未开始 |

**已可运行**：`docker compose up -d` 拉起 4 个容器，提交 Flink 作业后，Kafka 中的行为数据会经清洗落入 ClickHouse。链路已实测验证（含脏数据过滤）。

---

## 目标架构

```
┌─────────────────────────────────────────────────────────────┐
│  展示层   移动端 App  ←──────────→  Spring Boot REST API      │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  存储层   ClickHouse（分析结果） + MySQL（业务） + Redis（缓存）│
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  计算层   实时：Flink（清洗 + 窗口聚合）                       │
│           离线：Spark（漏斗 / 留存 / 分群）                    │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  采集层   Kafka（KRaft 模式，无需 Zookeeper）                  │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  数据源   UserBehavior 行为数据集                             │
└─────────────────────────────────────────────────────────────┘
```

**设计取舍**：为控制个人项目的维护成本，砍掉了 HDFS、Sqoop、DolphinScheduler、Zookeeper 等重型组件 —— 原始日志落本地磁盘，离线任务用 `@Scheduled` 触发。生产环境这些都有其价值，但个人项目里维护它们会挤占真正用于数据处理的时间。

---

## 技术栈

| 层 | 技术 | 版本 | 运行位置 |
|---|---|---|---|
| 语言 | Java | 17 | — |
| 构建 | Maven | 3.9 | — |
| 后端 | Spring Boot | 3.5.15 | 本地 |
| 消息队列 | Apache Kafka（KRaft） | 4.3.1 | Docker |
| 实时计算 | Apache Flink（Java API） | 2.3.0 | Docker |
| 分析存储 | ClickHouse | 24.3.18.7 | Docker |
| 业务库 | MySQL | 26.7.0 | 本地 |
| 缓存 | Redis | 8.10.1 | 本地 |
| 离线计算 | Apache Spark（Java API） | 4.2.0 | 本地 |

> **Flink 2.x 说明**：Flink 2.0 是一次破坏性大版本更新，`SourceFunction` / `SinkFunction` 已移入 `legacy` 包，`FlinkKafkaConsumer` 被 `KafkaSource` 取代 —— 网上多数教程仍是 1.x 写法，不可直接套用。

---

## 数据源

[UserBehavior 数据集](https://tianchi.aliyun.com/dataset/649)（阿里天池）：**3.4 GB / 100,150,807 行**，时间跨度 2017-11-25 ~ 2017-12-03。

无表头，5 列：

| 列 | 含义 |
|---|---|
| user_id | 用户 ID |
| item_id | 商品 ID |
| category_id | 商品类目 ID |
| behavior_type | 行为类型 |
| timestamp | Unix 秒级时间戳 |

**行为类型只有 4 种**：

```
pv（浏览） → fav（收藏）/ cart（加购） → buy（购买）
```

⚠️ 数据集中**没有独立的「下单」和「支付」事件**，购买即 `buy` 一步到位，因此漏斗是三段而非通常设想的五六段。

⚠️ 数据集**不含店铺维度**（只有 `category_id`），也**不含用户人口属性**。因此当前经营单元是**类目**而非店铺；店铺视角需要另外合成 `item → shop` 映射，属于后续工作。

**数据质量**：抽样 500 万行扫描，仅 23 行时间戳异常（约 0.0005%），无空字段、无格式错误。异常值在 Flink 清洗层过滤。

---

## 表结构

见 [`sql/clickhouse/01_schema.sql`](sql/clickhouse/01_schema.sql)。

| 层 | 表 | 内容 |
|---|---|---|
| DWD | `dwd_user_behavior` | 清洗后的行为明细，按天分区 |
| ADS | `ads_funnel_daily` | 每日转化漏斗 |
| ADS | `ads_item_stats` | 商品维度表现 |
| ADS | `ads_category_stats` | 类目维度表现 |
| ADS | `ads_active_user_daily` | 每日活跃用户 |
| ADS | `ads_realtime_pv_uv` | 实时 PV/UV（Flink 窗口写入） |

**时区约定**：派生列显式使用 `Asia/Shanghai`。ClickHouse 服务器默认 UTC，而数据集是淘宝的 —— 按 UTC 分天会把「一天」切成北京时间 8 点到次日 8 点，「每日活跃」这类指标就失真了。

---

## 模块结构

```
shop-insight/
├── shop-insight-job/        Flink / Spark 计算任务（独立提交）
├── sql/clickhouse/          ClickHouse 建表脚本
├── docker-compose.yml       Kafka + ClickHouse + Flink
└── src/                     Spring Boot 骨架
```

目标多模块结构（拆分进行中）：

```
shop-insight-common/     公共工具、统一响应体
shop-insight-dao/        数据访问（ClickHouse / MySQL / Redis）
shop-insight-service/    业务逻辑
shop-insight-web/        Spring Boot 启动模块 + REST API
shop-insight-job/        Flink / Spark 计算任务
```

---

## 快速开始

### 环境要求

- JDK 17、Maven 3.9+
- Docker Desktop（运行 Kafka / ClickHouse / Flink）
- 本机 MySQL 与 Redis（也可改由 Docker 承载）

### 启动基础设施

```bash
docker compose up -d
docker compose ps
```

启动 4 个容器：

| 容器 | 端口 |
|---|---|
| shop-insight-kafka | 9092 |
| shop-insight-clickhouse | 8123（HTTP）、9000（原生） |
| shop-insight-jobmanager | 8081（Flink Web UI） |
| shop-insight-taskmanager | — |

### 建表

```bash
docker exec -i shop-insight-clickhouse clickhouse-client \
  -u shop_insight --password shop_insight --multiquery < sql/clickhouse/01_schema.sql
```

### 构建并提交 Flink 作业

```bash
cd shop-insight-job
mvn clean package
docker cp target/shop-insight-job-0.0.1-SNAPSHOT.jar shop-insight-jobmanager:/tmp/job.jar
docker exec shop-insight-jobmanager /opt/flink/bin/flink run -d /tmp/job.jar
```

Flink Web UI：http://localhost:8081

---

## 路线图

- [x] 环境搭建
- [x] ClickHouse 表结构设计
- [x] Kafka → Flink → ClickHouse 链路打通
- [ ] 开启 checkpoint，实现失败恢复
- [ ] 全量数据导入（1 亿行）
- [ ] Flink 窗口计算（实时 PV/UV、热销榜）
- [ ] Spring Boot REST API
- [ ] Spark 离线计算（转化漏斗、复购、用户分群）
- [ ] 多模块拆分
- [ ] 移动端数据看板
- [ ] 调度与监控

---

## 许可

个人学习项目。
