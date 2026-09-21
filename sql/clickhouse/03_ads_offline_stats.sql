-- ============================================================================
-- 离线维度统计：商品 / 类目 / 品牌 / 日活
--
-- 【为什么用 ClickHouse SQL 而不是 Spark】
--
-- 这四个指标全是 `GROUP BY + countIf + uniqExact` 形态的聚合。
-- 项目里 FunnelJob 的类注释已经写过同样的话：
--   「这个漏斗本质是一个 GROUP BY 的简单聚合，ClickHouse 一条 SQL 就能算，而且更快。
--     用 Spark 的理由是架构上的。」
--
-- 一开始确实用 Spark 写了这四个作业，实测的问题：
--   · 每个作业都要全量扫 1.1 亿行，四个作业就是扫四遍（单次约 350 秒）
--   · 单机环境下 Spark 和 ClickHouse 抢同一台机器的内存
--   · JDBC 单分区读取把 1.1 亿行塞进一个 task，直接 OOM
--     （加 partitionColumn 之后读通过了，但 countDistinct 仍要
--       在 4.2 万个分组里各维护 530 万用户的去重集合，还是 OOM）
--
-- 换回 ClickHouse 之后是分钟级完成。**批处理层并没有因此消失** ——
-- 漏斗 / 留存 / RFM 三个作业仍在 Spark 里，其中 RFM 用 MLlib 做 KMeans 聚类，
-- 那才是 Spark 真正不可替代的地方（见 docs/项目详解.md §6）。
--
-- 【性能实测】类目聚合约 60 秒；四个合计约 4 分钟。
-- 对比：同样的事在 Spark 上单个作业就要 350 秒以上，还没算上重试。
--
-- 【⚠️ 日期过滤不是可选项】
-- dwd_user_behavior 里混着两类数据：原始导入落在 2019-10/11，
-- 而回放生产者做了时间戳平移，产生的数据落在 2026 年（约 1.15 亿条）。
-- 不加过滤会把回放噪声全部算进来。
--
-- 【重跑是幂等的】
-- 四张表都是 ReplacingMergeTree，排序键含 event_date（商品表是
-- (event_date, category_id, product_id)、类目表是 (event_date, category_id)、
-- 品牌表是 (event_date, brand)、日活表是 event_date），
-- 所以重跑时同一天的行会替换而不是追加。
-- ============================================================================

-- ---------------------------------------------------------------------------
-- ① 商品维度 → ads_item_stats
--
-- 用途：找爆款、找「看了不买」的商品（PV 高但转化率低）
--
-- 这张表还有个额外价值：/api/product-ranking 现在直接在 2.2 亿行明细上做全表聚合，
-- 实测 12~25 秒。有了它之后，同样的排行可以从明细表切到这张表算，降到毫秒级。
-- ---------------------------------------------------------------------------
INSERT INTO shop_insight.ads_item_stats
SELECT
    event_date,
    product_id,
    -- 一个商品只属于一个类目/品牌，取 max 只是为了拿到确定性的单值
    max(category_id)                            AS category_id,
    max(brand)                                  AS brand,
    countIf(event_type = 'view')                AS view_cnt,
    countIf(event_type = 'cart')                AS cart_cnt,
    countIf(event_type = 'purchase')            AS purchase_cnt,
    uniqExactIf(user_id, event_type = 'view')   AS view_uv,
    sumIf(price, event_type = 'purchase')       AS gmv,
    -- 购买转化率。view_cnt 为 0 时给 0（列是非空 Float64，不能给 NULL）
    if(countIf(event_type = 'view') > 0,
       countIf(event_type = 'purchase') / countIf(event_type = 'view'),
       0)                                        AS buy_rate
FROM shop_insight.dwd_user_behavior
WHERE event_date >= '2019-10-01' AND event_date <= '2019-11-30'
GROUP BY event_date, product_id;

-- ---------------------------------------------------------------------------
-- ② 类目维度 → ads_category_stats
--
-- 用途：哪个品类热、哪个品类「叫好不叫座」（PV 高但转化率低）
-- 类目是这个数据集里最接近「商家经营单元」的维度。
--
-- product_cnt 是当日该类目下【有行为的】商品数，不是全量商品数 ——
-- 数据里没有商品主数据表，只能按行为反推。
-- ---------------------------------------------------------------------------
INSERT INTO shop_insight.ads_category_stats
SELECT
    event_date,
    category_id,
    max(category_code)                          AS category_code,
    countIf(event_type = 'view')                AS view_cnt,
    countIf(event_type = 'cart')                AS cart_cnt,
    countIf(event_type = 'purchase')            AS purchase_cnt,
    uniqExactIf(user_id, event_type = 'view')   AS view_uv,
    uniqExact(product_id)                       AS product_cnt,
    sumIf(price, event_type = 'purchase')       AS gmv,
    if(countIf(event_type = 'view') > 0,
       countIf(event_type = 'purchase') / countIf(event_type = 'view'),
       0)                                        AS buy_rate
FROM shop_insight.dwd_user_behavior
WHERE event_date >= '2019-10-01' AND event_date <= '2019-11-30'
GROUP BY event_date, category_id;

-- ---------------------------------------------------------------------------
-- ③ 品牌维度 → ads_brand_stats
--
-- 用途：品牌是这个数据集里最接近「商家」的实体。
-- buyer_uv（购买用户数）配 view_uv 能看出「来的人多但成交少」的结构性问题。
--
-- 不过滤 brand = 'unknown' —— 数据集里约 14.5% 的行为没有品牌，
-- 全部保留，让表本身是完整的。过滤是展示层的事（API 已有 WHERE brand != 'unknown'）。
-- ---------------------------------------------------------------------------
INSERT INTO shop_insight.ads_brand_stats
SELECT
    event_date,
    brand,
    countIf(event_type = 'view')                        AS view_cnt,
    countIf(event_type = 'cart')                        AS cart_cnt,
    countIf(event_type = 'purchase')                    AS purchase_cnt,
    uniqExactIf(user_id, event_type = 'view')           AS view_uv,
    uniqExactIf(user_id, event_type = 'purchase')       AS buyer_uv,
    uniqExact(product_id)                               AS product_cnt,
    sumIf(price, event_type = 'purchase')               AS gmv,
    -- 客单价 = 成交额 / 购买数。购买数为 0 时给 NULL，不做除零
    if(countIf(event_type = 'purchase') > 0,
       round(sumIf(price, event_type = 'purchase') / countIf(event_type = 'purchase'), 2),
       NULL)                                            AS avg_price,
    if(countIf(event_type = 'view') > 0,
       countIf(event_type = 'purchase') / countIf(event_type = 'view'),
       0)                                                AS buy_rate
FROM shop_insight.dwd_user_behavior
WHERE event_date >= '2019-10-01' AND event_date <= '2019-11-30'
GROUP BY event_date, brand;

-- ---------------------------------------------------------------------------
-- ④ 每日活跃 → ads_active_user_daily
--
-- 用途：盘子有多大、在增还是在缩。所有经营分析的底板 ——
-- DAU 掉了之后再看转化率和品类结构才有意义。
--
-- new_user 是这里唯一需要【跨天】计算的指标：要判断「这天是不是某个用户
-- 第一次出现」，必须知道该用户在整个数据跨度内的最早活跃日期。
-- 所以先按 user_id 求 min(event_date)，再按那个日期分组计数，最后 join 回来。
-- ---------------------------------------------------------------------------
INSERT INTO shop_insight.ads_active_user_daily
SELECT
    d.event_date,
    d.dau,
    d.view_uv,
    d.purchase_uv,
    n.new_user,
    d.session_cnt
FROM (
    SELECT
        event_date,
        -- DAU = 有任意行为的去重用户数（不是只有浏览才算活跃）
        uniqExact(user_id)                          AS dau,
        uniqExactIf(user_id, event_type = 'view')   AS view_uv,
        uniqExactIf(user_id, event_type = 'purchase') AS purchase_uv,
        uniqExact(session_id)                       AS session_cnt
    FROM shop_insight.dwd_user_behavior
    WHERE event_date >= '2019-10-01' AND event_date <= '2019-11-30'
    GROUP BY event_date
) d
LEFT JOIN (
    SELECT first_date AS event_date, count() AS new_user
    FROM (
        SELECT user_id, min(event_date) AS first_date
        FROM shop_insight.dwd_user_behavior
        WHERE event_date >= '2019-10-01' AND event_date <= '2019-11-30'
        GROUP BY user_id
    )
    GROUP BY first_date
) n ON d.event_date = n.event_date
ORDER BY d.event_date;
