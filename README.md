# ShopInsight

> 基于 Lambda 架构的电商用户行为分析与商家经营决策平台

面向电商**商家经营决策**的分析平台：看清流量从哪来、在哪一环流失、哪些用户值得重点维护。

覆盖从数据采集到移动端展示的完整链路：行为日志经 Kafka 承接 → Flink 实时清洗与窗口计算 → ClickHouse 支撑多维查询 → Spark 离线计算经营指标 → Spring Boot 提供 REST API → Web / 移动端展示。

---

## 项目数据

| 项 | 值 |
|---|---|
| 数据规模 | **1.1 亿行** 用户行为明细（13.7 GB 原始 CSV） |
| 用户 / 商品 / 类目 / 品牌 | 531 万 / 20.7 万 / 691 / 4,304 |
| 时间跨度 | 2019-10-01 ~ 2019-11-30（61 天） |
| 实时链路 | Flink 1 分钟事件时间滚动窗口，按 **3 个维度**聚合（全局 / 类目 / 品牌） |
| 离线指标 | 转化漏斗、留存矩阵、RFM 用户分群 |
| 接口 | 10 个 REST 端点 |
| 展示端 | Web 看板（原生 HTML/SVG）+ Flutter 应用（5 个页面） |

**一个实测结论**：RFM 聚类发现 **8.2% 的高价值用户贡献了 76.6% 的成交额** —— 这是离线分析相对实时监控的核心价值：前者给决策依据，后者只给瞬时数字。

---

## 架构

```
┌──────────────────────────────────────────────────────────────────┐
│  展示层     Web 看板（HTML/SVG）  ←→  Flutter 应用（Web/移动端）    │
└──────────────────────────────────────────────────────────────────┘
                                 ↑ REST / JSON
┌──────────────────────────────────────────────────────────────────┐
│  服务层     Spring Boot 3.5  ·  10 个分析端点  ·  CORS 配置          │
└──────────────────────────────────────────────────────────────────┘
                                 ↑ JDBC
┌──────────────────────────────────────────────────────────────────┐
│  存储层     ClickHouse 24.3  ·  11 张表（明细 / 实时指标 / 离线指标）│
└──────────────────────────────────────────────────────────────────┘
                    ↑ 写入                        ↑ 写入
┌──────────────────────────────────┐  ┌────────────────────────────┐
│  实时计算  Flink 2.3              │  │  离线计算  Spark 4.2        │
│  · 清洗（4 类规则）                │  │  · 转化漏斗                 │
│  · 窗口聚合（全局/类目/品牌）       │  │  · 留存矩阵                 │
│  · checkpoint + exactly-once      │  │  · RFM 分群（MLlib KMeans）  │
└──────────────────────────────────┘  └────────────────────────────┘
                    ↑
┌──────────────────────────────────────────────────────────────────┐
│  采集层     Kafka 4.3（KRaft 模式，无需 Zookeeper）                 │
└──────────────────────────────────────────────────────────────────┘
                    ↑
┌──────────────────────────────────────────────────────────────────┐
│  数据源     行为日志回放生产者（模拟 App 埋点实时上报）              │
└──────────────────────────────────────────────────────────────────┘
```

### 为什么实时和离线都要有

两者回答不同的问题，不是重复建设：

| | 实时（Flink） | 离线（Spark） |
|---|---|---|
| 回答 | 「现在在发生什么」 | 「长期是什么样、下一步该怎么做」 |
| 更新 | 秒级 | T+1 |
| 能算的 | 窗口内可增量计算的指标 | 需要全量历史 / 用户级路径 / 全局排序的指标 |
| 例子 | 实时 PV/UV、分品牌成交额 | 留存曲线、RFM 分群、转化漏斗 |
| 商家用途 | 盯盘、大促监控 | 选品、定价、老客召回 |

> **为什么留存和 RFM 必须离线**：实时窗口只保留当前窗口的累加器，算不了「用户第 7 天还回不回来」这种跨越整段时间的用户级矩阵。

### 设计取舍

为控制个人项目的维护成本，砍掉了 HDFS、Sqoop、DolphinScheduler、Zookeeper 等重型组件 —— 原始数据落本地磁盘，离线任务用 `spark-submit` 手动触发。生产环境这些都有价值，但个人项目里维护它们会挤占真正用于数据处理的时间。

---

## 技术栈

| 层 | 技术 | 版本 | 运行位置 |
|---|---|---|---|
| 语言 | Java | 17 | — |
| 构建 | Maven | 3.9 | — |
| 消息队列 | Apache Kafka（KRaft） | 4.3.1 | Docker |
| 实时计算 | Apache Flink（Java API） | 2.3.0 | Docker |
| 分析存储 | ClickHouse | 24.3.18.7 | Docker |
| 离线计算 | Apache Spark（Java API + MLlib） | 4.2.0 | 本地 |
| 服务端 | Spring Boot | 3.5.15 | 本地 |
| 展示端 | Flutter / Dart | 3.47.2 | Web / 移动 |

### 两个版本陷阱（踩过）

**Flink 2.x 是破坏性大版本**：`SourceFunction` / `SinkFunction` 已移入 `legacy` 包，`FlinkKafkaConsumer` 被 `KafkaSource` 取代，`env.setRestartStrategy()` 被移除。网上多数教程仍是 1.x 写法，照抄会编译不过。

**Spark 4.x 用 Scala 2.13**，Maven 坐标是 `spark-sql_2.13`，不是老教程里的 `_2.12`。而且 **MLlib 是独立工件**（`spark-mllib_2.13`），不在 `spark-sql` 里。

---

## 数据源

[eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)：**13.7 GB / 约 1.1 亿行**，时间跨度 2019-10 至 2019-11。

9 列带表头：

```
event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
2019-11-01 00:00:00 UTC, view, 1003461, 2053013555631882655, electronics.smartphone, xiaomi, 489.07, 520088904, 4d3b30da-...
```

**行为类型只有 3 种**：

```
view（浏览） → cart（加购） → purchase（购买）
```

⚠️ 数据集中**没有独立的「支付」和「退款」事件**，`purchase` 即成交，因此漏斗是三段。

### ⚠️ 两个必须知道的字段陷阱

**1. `category_id` 必须用 UInt64**

实际最大值为 **2,180,736,567,012,753,664**（约 2.18e18），比 `UInt32` 上限（42.9 亿）**大 5 亿倍**。实测用 `UInt32` 会直接变成 `NULL`，整个类目维度报废。Flink 侧的 POJO 字段对应要用 `long` 而非 `int`。

**2. 空值不是脏数据**

`category_code` 约 32% 为空、`brand` 约 14.5% 为空 —— 这是数据集的固有特征，不是质量问题。导入时统一填 `unknown` 而不是丢弃，也不建议用 `Nullable`（会增加存储和查询开销）。

**时间戳格式**：`2019-11-01 00:00:00 UTC`，需剥掉时区后缀再解析。数据是面向多国用户的商城，统一用 UTC 不做本地转换。

---

## 数据模型

见 [`sql/clickhouse/01_schema.sql`](sql/clickhouse/01_schema.sql)。

### 分层

| 层 | 表 | 内容 |
|---|---|---|
| DWD | `dwd_user_behavior` | 清洗后的行为明细，按天分区 |

### 实时指标（Flink 窗口写入）

| 表 | 维度 |
|---|---|
| `ads_realtime_pv_uv` | 全局 |
| `ads_realtime_category_stats` | 按类目 |
| `ads_realtime_brand_stats` | 按品牌 |

### 离线指标（Spark 批处理写入）

| 表 | 内容 |
|---|---|
| `ads_funnel_daily` | 每日转化漏斗 |
| `ads_retention_daily` | 留存矩阵（批次 × 第 N 天） |
| `ads_user_profile` | RFM 分群（KMeans 聚类结果） |
| `ads_item_stats` / `ads_category_stats` / `ads_brand_stats` | 商品 / 类目 / 品牌维度表现 |
| `ads_active_user_daily` | 每日活跃用户 |

### 两个表设计决策

**① 全局和分类目/分品牌必须分表，不能加一列**

UV 是**去重计数**，一个用户可能访问多个类目，各类目 UV 之和 ≠ 全局 UV（实测 230,050 vs 202,511）。拆开后就再也算不出正确的全局 UV 了。

**② 所有 ADS 表用 `ReplacingMergeTree`**

批任务必须可以反复重跑。用普通 `MergeTree` 时，作业每次重启重算同一窗口会**追加**而非覆盖 —— 实测曾产生 31% 的重复行。

> 注意其去重是**后台异步**的，查询精确结果需要 `FINAL` 或对指标取 `max()`。

---

## 关键实现

### Flink 作业（`shop-insight-job/`）

一条输入流分四个出口：

```
Kafka ──→ 解析清洗 ──┬──→ 明细      → dwd_user_behavior
                     ├──→ 全局窗口   → ads_realtime_pv_uv
                     ├──→ 分类目窗口 → ads_realtime_category_stats
                     └──→ 分品牌窗口 → ads_realtime_brand_stats
```

**事件时间处理**：`forBoundedOutOfOrderness(30s)` 水位线 + 1 分钟滚动窗口。

**清洗规则**（4 类）：字段数校验、时间可解析、时间在合理范围、行为类型在枚举内。

### 数据回放生产者

读 CSV 按固定速率发到 Kafka，模拟实时埋点流（实测 8,100 条/秒）。

**两个必要的处理**，否则事件时间窗口会丢掉大部分数据：

1. **按时间戳排序后再发** —— CSV 是离线 dump，时间戳乱序。直接顺序发送时水位线会飞快推过窗口结束时间，之后的记录全被判为迟到丢弃（实测只统计到 **2.8%**）。
2. **全部发往单一分区** —— topic 有 3 个分区时 Flink 并发读，各分区进度会错开，水位线按最快的走，落后分区的数据被丢（实测捕获率升到 46.9% 后仍丢一半，单分区后 **100%**）。

> 真实场景里各分区是持续同步写入的，不会出现跨分区错位 —— 这是「把 83 天数据压缩到几十秒回放」带来的问题。生产环境若要并行应按 key 分区，保证同 key 有序。

### Spark 离线任务（`shop-insight-spark/`）

| 任务 | 输入 | 耗时 |
|---|---|---|
| `FunnelJob` | 1.1 亿行明细 | 252 秒 |
| `RetentionJob` | 1,500 万用户-日期对 | 39 秒 |
| `RfmUserProfileJob` | 531 万用户 | 177 秒 |

**RFM 用 MLlib 的 KMeans 聚类，不是分位数阈值** —— 这是批处理层存在的真正理由：分位数阈值一条 SQL 就能算，聚类不行。

三个实现细节：
- F 和 M 先 `log1p` 再标准化 —— 消费金额是长尾分布，不取对数的话普通用户会挤成一团
- 簇编号按各簇实际 R/F/M 均值**重新排序**后映射成业务名称，不能假设「簇 0 就是高价值」
- 固定随机种子（42），保证每次跑分群结果一致

### 展示端

**Web 看板**（`src/main/resources/static/index.html`）：单文件、零依赖、手写 SVG 图表。

**Flutter 应用**（`app/`）：5 个页面（实时总览 / 经营分析 / 品牌排行 / 类目排行 / 商品排行），响应式导航（宽屏侧栏、窄屏底栏）。

图表用 `fl_chart`。**没有用 ECharts 的原因**：Flutter 生态里没有活跃维护的 ECharts 组件 —— `flutter_echarts` 包已于 2023 年归档，作者推荐改用原生图表库。

调色板与 Web 看板一致，且跑过可视化规范的验证器（浅色/深色模式全部通过亮度带、色度、对比度检查）。

---

## 模块结构

```
shop-insight/
├── src/                      Spring Boot 服务端 + Web 看板
├── shop-insight-job/         Flink 实时任务 + 数据回放生产者
├── shop-insight-spark/       Spark 离线任务（独立模块）
├── app/                      Flutter 应用
├── sql/clickhouse/           建表脚本
├── clickhouse/config.d/      ClickHouse 配置（内存上限修正）
└── docker-compose.yml        Kafka + ClickHouse + Flink
```

> **Spark 为什么独立成模块**：Spark 和 Flink 都捆绑大量第三方库（Netty / Jackson / Scala），放在同一个 Maven 模块里会让编译期 classpath 出现版本冲突。

---

## 快速开始

### 环境要求

- JDK 17、Maven 3.9+
- Docker Desktop（运行 Kafka / ClickHouse / Flink）
- Spark 4.2.0（离线任务用，本地安装）
- Flutter 3.47.2（展示端用）

### 1. 启动基础设施

```bash
docker compose up -d
docker compose ps
```

| 容器 | 端口 |
|---|---|
| shop-insight-kafka | 9092 |
| shop-insight-clickhouse | 8123（HTTP）/ 9000（原生） |
| shop-insight-jobmanager | 8081（Flink Web UI） |
| shop-insight-taskmanager | — |

### 2. 建表

```bash
docker exec -i shop-insight-clickhouse clickhouse-client \
  -u shop_insight --password shop_insight --multiquery < sql/clickhouse/01_schema.sql
```

### 3. 导入历史数据

```bash
cat 2019-Oct.csv | docker exec -i shop-insight-clickhouse clickhouse-client \
  -u shop_insight --password shop_insight \
  --input_format_csv_skip_first_lines=1 \
  --query "
INSERT INTO shop_insight.dwd_user_behavior
  (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, session_id)
SELECT
    parseDateTimeBestEffort(raw_time), event_type,
    toUInt32OrZero(product_id), toUInt64OrZero(category_id),
    if(category_code='','unknown',category_code), if(brand='','unknown',brand),
    toDecimal64OrZero(price,2), toUInt32OrZero(user_id), session_id
FROM input('raw_time String, event_type String, product_id String, category_id String,
            category_code String, brand String, price String, user_id String, session_id String')
FORMAT CSV"
```

> 需要 `--input_format_allow_errors_num` 容错参数 —— 数据集里有极少数负时间戳的脏行。

### 4. 启动实时链路

```bash
# 提交 Flink 作业
cd shop-insight-job && mvn clean package
docker cp target/shop-insight-job-0.0.1-SNAPSHOT.jar shop-insight-jobmanager:/tmp/job.jar
docker exec shop-insight-jobmanager /opt/flink/bin/flink run -d /tmp/job.jar

# 回放数据（模拟实时流）
java -cp target/shop-insight-job-0.0.1-SNAPSHOT.jar \
  com.company.shopinsight.producer.BehaviorLogProducer \
  ../2019-Nov.csv localhost:9092 user_behavior_log 8000 300000
```

### 5. 运行离线任务

```bash
cd shop-insight-spark && mvn clean package
CH_JAR=~/.m2/repository/com/clickhouse/clickhouse-jdbc/0.10.0/clickhouse-jdbc-0.10.0-all.jar

for JOB in FunnelJob RetentionJob RfmUserProfileJob; do
  spark-submit --class com.company.shopinsight.spark.$JOB \
    --jars "$CH_JAR" target/shop-insight-spark-0.0.1-SNAPSHOT.jar
done
```

### 6. 启动服务端与展示端

```bash
# 服务端 + Web 看板
mvn clean package -DskipTests && java -jar target/shop-insight-0.0.1-SNAPSHOT.jar
# → http://localhost:8080/           Web 看板
# → http://localhost:8080/api/overview

# Flutter 应用
cd app && flutter run -d chrome
```

---

## API

| 端点 | 说明 |
|---|---|
| `GET /api/overview` | 实时总览（PV / UV / 购买数 / 成交额） |
| `GET /api/category-stats` | 分类目实时统计 |
| `GET /api/brand-stats` | 分品牌实时统计 |
| `GET /api/brand-ranking` | 品牌成交额排行 |
| `GET /api/category-ranking` | 类目成交额排行 |
| `GET /api/product-ranking` | 商品成交额排行 |
| `GET /api/funnel` | 每日转化漏斗（离线） |
| `GET /api/retention` | 留存曲线（离线） |
| `GET /api/user-segments` | RFM 用户分群（离线） |

> `brand-ranking` / `category-ranking` 刻意**不返回 UV** —— 跨窗口的 UV 不可加，同一用户会在多个窗口被重复计数。需要准确 UV 时应从明细表现算。

---

## 路线图

- [x] 环境搭建（Docker / Maven / JDK）
- [x] 表结构设计
- [x] Kafka → Flink → ClickHouse 实时链路打通
- [x] Flink 窗口计算（全局 / 类目 / 品牌三个维度）
- [x] Checkpoint + 失败恢复
- [x] 全量数据导入（1.1 亿行）
- [x] Spark 离线计算（转化漏斗 / 留存 / RFM 分群）
- [x] Spring Boot REST API（10 个端点）
- [x] Web 看板
- [x] Flutter 应用（Web 端，5 个页面）
- [ ] Flutter Android 端
- [ ] 调度自动化（cron / Spring `@Scheduled`）
- [ ] 监控（Prometheus + Grafana）
- [ ] 多模块拆分（common / dao / service / web 分层）

---

## 已知限制

| 限制 | 说明 |
|---|---|
| Spark 读取未并行 | 漏斗任务读取 1.1 亿行用单分区 JDBC，252 秒。加 `numPartitions` + `partitionColumn` 可显著提速 |
| 离线任务手动触发 | 尚未接入调度，需要手动 `spark-submit` |
| Flutter 仅 Web 端 | Android 端需要模拟器；macOS 桌面端需要完整 Xcode |
| 无用户人口属性 | 数据集不含性别/年龄/地域，用户画像用 RFM 行为分群替代 |

---

## 许可

个人学习项目。
