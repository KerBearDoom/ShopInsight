-- =============================================================================
-- ShopInsight —— ClickHouse 表结构
--
-- 数据来源：UserBehavior.csv（天池淘宝用户行为数据集）
--   3.4GB / 100,150,807 行 / 2017-11-25 ~ 2017-12-03
--   原始 5 列无表头：user_id, item_id, category_id, behavior_type, timestamp
--
-- ⚠️ 这份数据没有 shop_id（店铺）维度，只有 category_id（类目）。
--    所以经营单元用「类目」而不是「店铺」，见 ADS 层的说明。
--
-- 执行方式：
--   docker exec -i shop-insight-clickhouse clickhouse-client \
--     -u shop_insight --password shop_insight --multiquery < 01_schema.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS shop_insight;


-- -----------------------------------------------------------------------------
-- DWD 层：行为明细
--
-- 直接对应 CSV 的 5 列，event_time / event_date 是派生列，靠 DEFAULT 表达式
-- 自动算出 —— 这样用 clickhouse-client 灌 CSV 时不用预处理。
--
-- 行为类型只有 4 种，不是通常设想的 6 种漏斗：
--   pv   浏览
--   fav  收藏
--   cart 加购
--   buy  购买       ← 没有独立的 order / pay / refund
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.dwd_user_behavior
(
    user_id     UInt32                 COMMENT '用户 ID',
    item_id     UInt32                 COMMENT '商品 ID',
    category_id UInt32                 COMMENT '类目 ID',
    behavior    LowCardinality(String) COMMENT '行为类型：pv / fav / cart / buy',
    event_ts    UInt32                 COMMENT 'Unix 秒时间戳（原始值）',
    event_time  DateTime DEFAULT toDateTime(event_ts) COMMENT '事件时间（派生）',
    event_date  Date     DEFAULT toDate(event_time)   COMMENT '事件日期（派生），也是分区键'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_date, item_id, user_id)
SETTINGS index_granularity = 8192;


-- -----------------------------------------------------------------------------
-- ADS 层 1：每日转化漏斗
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_funnel_daily
(
    event_date  Date    COMMENT '统计日期',
    pv_cnt      UInt64  COMMENT '浏览数',
    fav_cnt     UInt64  COMMENT '收藏数',
    cart_cnt    UInt64  COMMENT '加购数',
    buy_cnt     UInt64  COMMENT '购买数',
    pv_uv       UInt64  COMMENT '浏览用户数',
    buy_uv      UInt64  COMMENT '购买用户数',
    cart_rate   Float64 COMMENT '加购率 = cart / pv',
    buy_rate    Float64 COMMENT '购买转化率 = buy / pv'
)
ENGINE = MergeTree
ORDER BY event_date
COMMENT '商家视角核心表：定位流失环节';


-- -----------------------------------------------------------------------------
-- ADS 层 2：商品维度表现
--
-- 对应「商品流量看板」—— 哪些商品有吸引力、哪些需要优化
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_item_stats
(
    event_date  Date    COMMENT '统计日期',
    item_id     UInt32  COMMENT '商品 ID',
    category_id UInt32  COMMENT '所属类目',
    pv_cnt      UInt64  COMMENT '浏览量',
    fav_cnt     UInt64  COMMENT '收藏量',
    cart_cnt    UInt64  COMMENT '加购量',
    buy_cnt     UInt64  COMMENT '购买量',
    pv_uv       UInt64  COMMENT '独立访客数',
    cart_rate   Float64 COMMENT '加购率',
    buy_rate    Float64 COMMENT '购买转化率'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_id, item_id);


-- -----------------------------------------------------------------------------
-- ADS 层 3：类目维度表现
--
-- 这是「店铺经营看板」的替代品 —— 数据里没有 shop_id，
-- 所以经营单元是类目。如果以后要店铺视角，需要另外合成 item→shop 映射。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_category_stats
(
    event_date   Date    COMMENT '统计日期',
    category_id  UInt32  COMMENT '类目 ID',
    pv_cnt       UInt64  COMMENT '浏览量',
    fav_cnt      UInt64  COMMENT '收藏量',
    cart_cnt     UInt64  COMMENT '加购量',
    buy_cnt      UInt64  COMMENT '购买量',
    pv_uv        UInt64  COMMENT '独立访客数',
    item_cnt     UInt32  COMMENT '当日在售/有行为的商品数',
    cart_rate    Float64 COMMENT '加购率',
    buy_rate     Float64 COMMENT '购买转化率'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_id);


-- -----------------------------------------------------------------------------
-- ADS 层 4：每日活跃用户
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_active_user_daily
(
    event_date  Date   COMMENT '统计日期',
    dau         UInt64 COMMENT '日活跃用户数（任意行为）',
    pv_uv       UInt64 COMMENT '有浏览行为的用户数',
    buy_uv      UInt64 COMMENT '有购买行为的用户数',
    pv_cnt      UInt64 COMMENT 'PV 总量'
)
ENGINE = MergeTree
ORDER BY event_date;


-- -----------------------------------------------------------------------------
-- ADS 层 5：实时 PV/UV（全局）—— Flink 窗口写入
--
-- 用 ReplacingMergeTree 而不是 MergeTree：作业重启后重算同一个窗口时，
-- 新行会【替换】旧行而不是追加。用普通 MergeTree 时实测产生了 31% 的重复行
-- （期间反复重启作业调试）。排序键 window_start 相同的行会被合并。
--
-- 注意：ReplacingMergeTree 的去重是【后台异步】的，查询时如需精确结果
-- 要用 SELECT ... FINAL，或对指标取 max()/any() 去重。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_realtime_pv_uv
(
    window_start DateTime('Asia/Shanghai') COMMENT '窗口开始时间',
    window_end   DateTime('Asia/Shanghai') COMMENT '窗口结束时间',
    pv           UInt64   COMMENT '窗口内 PV',
    uv           UInt64   COMMENT '窗口内 UV（去重 user_id）',
    buy_cnt      UInt64   COMMENT '窗口内购买数'
)
ENGINE = ReplacingMergeTree
ORDER BY window_start;


-- -----------------------------------------------------------------------------
-- ADS 层 6：实时 PV/UV（分类目）—— Flink 窗口写入
--
-- 为什么要和全局表分开而不是加一列 category_id：
--   UV 是【去重计数】，不能跨类目相加 —— 一个用户可能访问多个类目，
--   各类目 UV 之和 ≠ 全局 UV。所以全局和分类目必须各自独立计算。
--
-- 这张表让「实时总览」有了经营单元：运营能看到【自己的类目】的实时流量，
-- 而不只是全站总量。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_insight.ads_realtime_category_stats
(
    window_start DateTime('Asia/Shanghai') COMMENT '窗口开始时间',
    window_end   DateTime('Asia/Shanghai') COMMENT '窗口结束时间',
    category_id  UInt32   COMMENT '类目 ID',
    pv           UInt64   COMMENT '窗口内该类目的 PV',
    uv           UInt64   COMMENT '窗口内该类目的 UV（去重 user_id）',
    buy_cnt      UInt64   COMMENT '窗口内该类目的购买数'
)
ENGINE = ReplacingMergeTree
ORDER BY (window_start, category_id);
