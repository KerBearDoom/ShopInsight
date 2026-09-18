-- =============================================================================
-- ShopInsight —— ClickHouse 表结构
--
-- 数据源：数据集1（eCommerce behavior data from multi category store）
--   13.7 GB / 约 1.1 亿行 / 2019-10-01 ~ 2019-11-30
--   9 列带表头：event_time, event_type, product_id, category_id, category_code,
--               brand, price, user_id, user_session
--
-- 规模：110 万用户 / 13.4 万商品 / 624 类目 / 3,375 品牌
--
-- 行为只有 3 种：view（浏览）→ cart（加购）→ purchase（购买）
--   注意：没有独立的「支付」和「退款」，purchase 即成交
--
-- ⚠️ 关键字段类型说明：
--   category_id 的值可达 2.18e18，**必须用 UInt64**。
--   用 UInt32（上限 4.29e9）会直接变成 NULL —— 实测 toUInt32OrNull 返回 \N。
--   product_id (max 6210 万) 和 user_id (max 5.68 亿) 用 UInt32 足够。
--
-- ⚠️ 空值：category_code 约 32% 为空，brand 约 14.5% 为空。
--   导入时统一填 'unknown' 而不是用 Nullable —— Nullable 会增加存储和查询开销，
--   而这两个字段的"未知"在分析上语义一致（值得单独看，但不该让聚合变复杂）。
--
-- 执行：
--   docker exec -i shop-insight-clickhouse clickhouse-client \
--     -u shop_insight --password shop_insight --multiquery < 01_schema.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS shop_insight;


-- -----------------------------------------------------------------------------
-- DWD 层：行为明细
--
-- event_date 是派生列，用 DEFAULT 表达式自动算出，导入 CSV 时不用提供。
--
-- 时区说明：源数据是 UTC（格式为 "2019-11-01 00:00:00 UTC"），
-- 且这是一个面向多国用户的商城，所以保持 UTC 不做转换。
-- （之前的淘宝数据集是单国数据，所以当时用 Asia/Shanghai。）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.dwd_user_behavior
(
    event_time    DateTime('UTC')          COMMENT '事件时间（源数据为 UTC）',
    event_type    LowCardinality(String)   COMMENT '行为：view / cart / purchase',
    product_id    UInt32                   COMMENT '商品 ID',
    category_id   UInt64                   COMMENT '类目 ID（数值达 2.18e18，必须 UInt64）',
    category_code LowCardinality(String)   COMMENT '层级类目码，如 electronics.smartphone；空值填 unknown',
    brand         LowCardinality(String)   COMMENT '品牌；空值填 unknown',
    price         Decimal(10, 2)           COMMENT '该商品在事件发生时的价格（美元）',
    user_id       UInt32                   COMMENT '用户 ID',
    session_id    String                   COMMENT '会话 ID（UUID）',
    event_date    Date DEFAULT toDate(event_time) COMMENT '事件日期（派生），也是分区键'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_date, category_id, product_id)
SETTINGS index_granularity = 8192;


-- -----------------------------------------------------------------------------
-- ADS 层 1：每日转化漏斗（全局）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_funnel_daily
(
    event_date   Date    COMMENT '统计日期',
    view_cnt     UInt64  COMMENT '浏览数',
    cart_cnt     UInt64  COMMENT '加购数',
    purchase_cnt UInt64  COMMENT '购买数',
    view_uv      UInt64  COMMENT '浏览用户数',
    cart_uv      UInt64  COMMENT '加购用户数',
    purchase_uv  UInt64  COMMENT '购买用户数',
    gmv          Decimal(18, 2) COMMENT '成交额 = sum(price) where purchase',
    cart_rate    Float64 COMMENT '加购率 = cart / view',
    buy_rate     Float64 COMMENT '购买率 = purchase / view'
)
ENGINE = ReplacingMergeTree
ORDER BY event_date
COMMENT '全局转化漏斗，定位流失环节。用 ReplacingMergeTree 让批任务可以反复重跑';


-- -----------------------------------------------------------------------------
-- ADS 层 2：商品维度表现
--
-- 对应「商品流量看板」—— 哪些商品有吸引力、哪些需要优化
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_item_stats
(
    event_date   Date    COMMENT '统计日期',
    product_id   UInt32  COMMENT '商品 ID',
    category_id  UInt64  COMMENT '所属类目',
    brand        LowCardinality(String) COMMENT '品牌',
    view_cnt     UInt64  COMMENT '浏览数',
    cart_cnt     UInt64  COMMENT '加购数',
    purchase_cnt UInt64  COMMENT '购买数',
    view_uv      UInt64  COMMENT '独立访客数',
    gmv          Decimal(18, 2) COMMENT '成交额',
    buy_rate     Float64 COMMENT '购买转化率 = purchase / view'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_id, product_id);


-- -----------------------------------------------------------------------------
-- ADS 层 3：类目维度表现
--
-- 类目是现有数据下最接近「经营单元」的粒度（没有店铺字段）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_category_stats
(
    event_date    Date    COMMENT '统计日期',
    category_id   UInt64  COMMENT '类目 ID',
    category_code LowCardinality(String) COMMENT '层级类目码',
    view_cnt      UInt64  COMMENT '浏览数',
    cart_cnt      UInt64  COMMENT '加购数',
    purchase_cnt  UInt64  COMMENT '购买数',
    view_uv       UInt64  COMMENT '独立访客数',
    product_cnt   UInt32  COMMENT '当日在售/有行为的商品数',
    gmv           Decimal(18, 2) COMMENT '成交额',
    buy_rate      Float64 COMMENT '购买转化率'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_id);


-- -----------------------------------------------------------------------------
-- ADS 层 4：品牌维度表现 ⭐
--
-- 这是数据集1 相比淘宝数据最大的增量 —— brand 字段让「商家经营视角」成立。
-- 品牌就是数据里最接近「商家」的实体。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_brand_stats
(
    event_date   Date    COMMENT '统计日期',
    brand        LowCardinality(String) COMMENT '品牌',
    view_cnt     UInt64  COMMENT '浏览数',
    cart_cnt     UInt64  COMMENT '加购数',
    purchase_cnt UInt64  COMMENT '购买数',
    view_uv      UInt64  COMMENT '独立访客数',
    buyer_uv     UInt64  COMMENT '购买用户数',
    product_cnt  UInt32  COMMENT '有行为的商品数',
    gmv          Decimal(18, 2) COMMENT '成交额',
    avg_price    Decimal(10, 2) COMMENT '客单价 = gmv / 购买数',
    buy_rate     Float64 COMMENT '购买转化率'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, brand);


-- -----------------------------------------------------------------------------
-- ADS 层 5：每日活跃用户
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_active_user_daily
(
    event_date  Date   COMMENT '统计日期',
    dau         UInt64 COMMENT '日活跃用户数（任意行为）',
    view_uv     UInt64 COMMENT '有浏览行为的用户数',
    purchase_uv UInt64 COMMENT '有购买行为的用户数',
    new_user    UInt64 COMMENT '当日首次出现的用户数',
    session_cnt UInt64 COMMENT '会话数'
)
ENGINE = ReplacingMergeTree
ORDER BY event_date;


-- -----------------------------------------------------------------------------
-- ADS 层 6：实时 PV/UV（全局）—— Flink 窗口写入
--
-- 用 ReplacingMergeTree：作业重启后重算同一窗口时【替换】而非追加。
-- 用普通 MergeTree 时实测产生过 31% 的重复行。
-- 注意其去重是后台异步的，查询精确结果需 FINAL 或对指标取 max()。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_realtime_pv_uv
(
    window_start DateTime('UTC') COMMENT '窗口开始时间',
    window_end   DateTime('UTC') COMMENT '窗口结束时间',
    pv           UInt64 COMMENT '窗口内 PV',
    uv           UInt64 COMMENT '窗口内 UV（去重 user_id）',
    purchase_cnt UInt64 COMMENT '窗口内购买数',
    gmv          Decimal(18, 2) COMMENT '窗口内成交额'
)
ENGINE = ReplacingMergeTree
ORDER BY window_start;


-- -----------------------------------------------------------------------------
-- ADS 层 7：实时 PV/UV（分类目）—— Flink 窗口写入
--
-- 为什么不给全局表加一列 category_id 而是分开两张表：
--   UV 是【去重计数】，一个用户可能访问多个类目，各类目 UV 之和 ≠ 全局 UV。
--   实测（淘宝数据）230,050 vs 202,511，拆开后就再也算不出正确的全局 UV。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_realtime_category_stats
(
    window_start DateTime('UTC') COMMENT '窗口开始时间',
    window_end   DateTime('UTC') COMMENT '窗口结束时间',
    category_id  UInt64 COMMENT '类目 ID',
    pv           UInt64 COMMENT '窗口内该类目的 PV',
    uv           UInt64 COMMENT '窗口内该类目的 UV（去重 user_id）',
    purchase_cnt UInt64 COMMENT '窗口内该类目的购买数',
    gmv          Decimal(18, 2) COMMENT '窗口内该类目的成交额'
)
ENGINE = ReplacingMergeTree
ORDER BY (window_start, category_id);


-- -----------------------------------------------------------------------------
-- ADS 层 8：实时品牌榜 —— Flink 窗口写入
--
-- 品牌是「商家」的代理，这张表对应「实时热销榜」的商家视角。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_realtime_brand_stats
(
    window_start DateTime('UTC') COMMENT '窗口开始时间',
    window_end   DateTime('UTC') COMMENT '窗口结束时间',
    brand        LowCardinality(String) COMMENT '品牌',
    pv           UInt64 COMMENT '窗口内 PV',
    uv           UInt64 COMMENT '窗口内 UV',
    purchase_cnt UInt64 COMMENT '窗口内购买数',
    gmv          Decimal(18, 2) COMMENT '窗口内成交额'
)
ENGINE = ReplacingMergeTree
ORDER BY (window_start, brand);


-- -----------------------------------------------------------------------------
-- ADS 层 9：用户分群（RFM）—— Spark 离线写入
--
-- 替代做不了的人口画像（数据集不含性别/年龄/地域）。
--
-- 分子段的方式是 **Spark MLlib 的 KMeans 聚类**，不是写死的分位数阈值 ——
-- 这是 Spark 在这个项目里真正不可替代的地方：
--   分位数阈值用一条 SQL 就能算，聚类不行。
--
-- ⚠️ M（Monetary）是**消费金额**，数据集1 有 price 字段所以是真实金额，
-- 不是之前淘宝数据集那种「用购买次数代理」的降级方案。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_user_profile
(
    user_id       UInt32   COMMENT '用户 ID',
    recency_days  Int32    COMMENT 'R：最近一次行为距参考日的天数，越小越活跃',
    frequency     UInt32   COMMENT 'F：行为总次数',
    monetary      Decimal(18, 2) COMMENT 'M：消费金额',
    r_score       UInt8    COMMENT 'R 得分 1-5（5 最好）',
    f_score       UInt8    COMMENT 'F 得分 1-5',
    m_score       UInt8    COMMENT 'M 得分 1-5',
    cluster_id    UInt8    COMMENT 'KMeans 聚出的簇编号',
    segment       LowCardinality(String) COMMENT '分群名称：高价值 / 潜力 / 流失风险 / 一般保持',
    computed_at   DateTime('UTC') COMMENT '计算时间'
)
ENGINE = ReplacingMergeTree
ORDER BY user_id
COMMENT '用户 RFM 分群，由 Spark MLlib KMeans 聚类得出';


-- -----------------------------------------------------------------------------
-- ADS 层 10：留存（按天分批次）—— Spark 离线写入
--
-- 形状是「批次 × 第 N 天」的矩阵，不是单日活跃数 —— 所以装不进 ads_active_user_daily。
--
-- 为什么必须离线：留存要回答「用户首次出现后，第 N 天还回不回来」，
-- 需要跨越整段时间的用户维度的矩阵，实时窗口的状态装不下。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_retention_daily
(
    cohort_date    Date    COMMENT '批次日期：用户在这一天首次/重新活跃',
    day_offset     UInt16  COMMENT '第 N 天（0 = 当天）',
    cohort_users   UInt32  COMMENT '该批次的用户数',
    retained_users UInt32  COMMENT '第 N 天仍活跃的用户数',
    retention_rate Float64 COMMENT '留存率 = retained / cohort'
)
ENGINE = ReplacingMergeTree
ORDER BY (cohort_date, day_offset)
COMMENT '按首次活跃日期分组的留存矩阵';
