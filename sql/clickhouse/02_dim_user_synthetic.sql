-- =============================================================================
-- ShopInsight —— 用户维度表（模拟生成）
--
-- ⚠️ 这张表的数据是【生成】的，不是真实的。
--
-- 为什么需要它：UserBehavior / 数据集1 只含行为字段（谁在何时对哪个商品做了什么），
-- 没有设备、地域、年龄、性别。为了让「用户画像」和「地域/设备分布」这两类展示
-- 有数据可渲染，这里按 user_id 稳定地生成一组属性。
--
-- ⚠️ 使用约束：
--   1. 只用于展示层渲染与联调，**不能用于任何业务结论**
--   2. 任何对外说明中都必须标注这部分为模拟数据
--   3. 不要把这些字段 JOIN 进对外发布的报表
--
-- 设计要点：**按 user_id 生成，不是按行生成**
--   用 cityHash64(user_id, '<字段名>') 做种子，保证同一个用户在任何时候、
--   任何表里拿到的都是同一组属性。如果按行随机，同一个 user_id 会同时出现在
--   iOS 和 Android 里，展示端一算就会自相矛盾（各设备用户数之和 ≠ 总用户数）。
--   加不同的盐（'device' / 'country' / …）是为了让各字段相互独立 ——
--   不加盐的话所有字段都由同一个哈希派生，会出现"iOS 用户全是女性"这类假相关。
--
-- 执行：
--   docker exec -i shop-insight-clickhouse clickhouse-client \
--     -u shop_insight --password shop_insight --multiquery < 02_dim_user_synthetic.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS shop_insight.dim_user_synthetic
(
    user_id     UInt32                 COMMENT '用户 ID（与 dwd_user_behavior 对应）',
    device      LowCardinality(String) COMMENT '设备类型：iOS / Android / Web —— 模拟',
    os          LowCardinality(String) COMMENT '操作系统 —— 模拟',
    browser     LowCardinality(String) COMMENT '浏览器 —— 模拟',
    country     LowCardinality(String) COMMENT '国家 —— 模拟',
    province    LowCardinality(String) COMMENT '省/州 —— 模拟',
    age_group   LowCardinality(String) COMMENT '年龄段 —— 模拟',
    gender      LowCardinality(String) COMMENT '性别 —— 模拟'
)
ENGINE = ReplacingMergeTree
ORDER BY user_id
COMMENT '用户维度（模拟生成，仅供展示层使用）';


INSERT INTO shop_insight.dim_user_synthetic
SELECT
    user_id,

    -- 设备：iOS 35% / Android 45% / Web 20%
    multiIf(h_dev < 35, 'iOS', h_dev < 80, 'Android', 'Web') AS device,

    -- 操作系统由设备决定，保证不出现「Android 设备跑 iOS」这种矛盾
    if(device = 'Web', if(h_os < 55, 'Windows', 'macOS'), device) AS os,

    -- 浏览器受设备约束：iOS 只能用 Safari/Chrome，Android 用 Chrome/Firefox
    multiIf(
        device = 'iOS',     if(h_br < 70, 'Safari', 'Chrome'),
        device = 'Android', if(h_br < 82, 'Chrome', 'Firefox'),
        multiIf(h_br < 60, 'Chrome', h_br < 80, 'Edge', h_br < 92, 'Firefox', 'Safari')
    ) AS browser,

    -- 国家：按真实跨境电商的大致流量分布加权
    multiIf(
        h_ctry < 55, '中国',
        h_ctry < 67, '美国',
        h_ctry < 77, '印度',
        h_ctry < 83, '德国',
        h_ctry < 88, '英国',
        h_ctry < 92, '巴西',
        h_ctry < 96, '日本',
                     '其他'
    ) AS country,

    -- 省份：中国细分到省，美国细分到州，其余给个占位
    multiIf(
        country = '中国',
            arrayElement(['广东','浙江','江苏','山东','河南','四川','北京','上海','湖北','福建'],
                         h_prov % 10 + 1),
        country = '美国',
            arrayElement(['California','Texas','New York','Florida','Illinois'],
                         h_prov % 5 + 1),
        '海外其他'
    ) AS province,

    -- 年龄段：中间大两头小，贴近真实电商分布
    multiIf(h_age < 22, '18-24', h_age < 60, '25-34', h_age < 85, '35-44', '45+') AS age_group,

    if(h_gen < 58, '女', '男') AS gender

FROM
(
    SELECT
        user_id,
        -- 每个字段一个独立的盐，避免字段之间产生假相关
        cityHash64(user_id, 'device')  % 100 AS h_dev,
        cityHash64(user_id, 'os')      % 100 AS h_os,
        cityHash64(user_id, 'browser') % 100 AS h_br,
        cityHash64(user_id, 'country') % 100 AS h_ctry,
        cityHash64(user_id, 'prov')    % 100 AS h_prov,
        cityHash64(user_id, 'age')     % 100 AS h_age,
        cityHash64(user_id, 'gender')  % 100 AS h_gen
    FROM (SELECT DISTINCT user_id FROM shop_insight.dwd_user_behavior)
);


-- -----------------------------------------------------------------------------
-- 验证：分布应当接近设定的权重，且各字段组合无矛盾
-- -----------------------------------------------------------------------------
-- SELECT device, count() FROM shop_insight.dim_user_synthetic FINAL GROUP BY device;
-- SELECT country, count() FROM shop_insight.dim_user_synthetic FINAL GROUP BY country ORDER BY count() DESC;
-- -- 不应出现 Android 设备配 iOS 系统：
-- SELECT count() FROM shop_insight.dim_user_synthetic FINAL WHERE device='Android' AND os<>'Android';
