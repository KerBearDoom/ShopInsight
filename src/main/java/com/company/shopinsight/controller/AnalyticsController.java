package com.company.shopinsight.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 数据分析接口。数据全部来自 ClickHouse 的实时窗口指标表。
 *
 * <p>数据源是数据集1（eCommerce behavior data），时间戳为 UTC，
 * 所以这里统一按 UTC 格式化输出，不做本地时区转换。
 *
 * <p><b>关于 ReplacingMergeTree 的读取方式</b>：这几张 ADS 表用 ReplacingMergeTree，
 * 同一个主键被重复写入时保留<strong>最新版本</strong>。它的去重是后台异步的，
 * 未合并前查询会看到重复行 —— 所以必须加 {@code FINAL} 强制合并后再读。
 *
 * <p><b>⚠️ 不要用 {@code GROUP BY 主键 + max()}。</b> 这个写法看起来能去重，
 * 但取的是<strong>最大值</strong>而不是<strong>最新值</strong>。实测踩过：
 * 回放生产者每轮把数据映射到「最近」，导致同一个窗口时间被多轮写入不同的值，
 * 用 max() 读出来的数字比真实值大 38 倍（243,431 vs 6,373）。
 */
@RestController
@RequestMapping("/api")
public class AnalyticsController {

    private static final String TIME_FMT = "%Y-%m-%d %H:%i:%S";

    private final JdbcTemplate clickHouse;

    public AnalyticsController(JdbcTemplate clickHouseJdbcTemplate) {
        this.clickHouse = clickHouseJdbcTemplate;
    }

    /**
     * 实时总览：最近 N 个窗口的全局 PV / UV / 购买数 / 成交额。
     *
     * <p>对应展示端「实时总览」页面。
     */
    @GetMapping("/overview")
    public List<Map<String, Object>> overview(@RequestParam(defaultValue = "30") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    formatDateTime(window_start, ?, 'UTC') AS window_start,
                    pv, uv, purchase_cnt,
                    round(gmv, 2) AS gmv
                FROM ads_realtime_pv_uv FINAL
                ORDER BY window_start DESC
                LIMIT ?
                """, TIME_FMT, limit);
    }

    /**
     * 分类目实时统计：最近窗口内各类目的流量与转化。
     *
     * <p>对应展示端「类目分析」页面。
     */
    @GetMapping("/category-stats")
    public List<Map<String, Object>> categoryStats(@RequestParam(defaultValue = "20") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    formatDateTime(window_start, ?, 'UTC') AS window_start,
                    category_id, pv, uv, purchase_cnt,
                    round(gmv, 2) AS gmv
                FROM ads_realtime_category_stats FINAL
                ORDER BY window_start DESC, gmv DESC
                LIMIT ?
                """, TIME_FMT, limit);
    }

    /**
     * 分品牌实时统计 ⭐
     *
     * <p><b>这是「商家经营视角」的入口</b> —— 品牌是数据集里最接近「商家」的实体。
     * 对应展示端「品牌经营」页面。
     */
    @GetMapping("/brand-stats")
    public List<Map<String, Object>> brandStats(@RequestParam(defaultValue = "20") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    formatDateTime(window_start, ?, 'UTC') AS window_start,
                    brand, pv, uv, purchase_cnt,
                    round(gmv, 2) AS gmv
                FROM ads_realtime_brand_stats FINAL
                ORDER BY window_start DESC, gmv DESC
                LIMIT ?
                """, TIME_FMT, limit);
    }

    /**
     * 品牌成交额排行（跨窗口汇总）。
     *
     * <p><b>为什么这里不返回 UV</b>：UV 是去重计数，<strong>跨窗口不可加</strong> ——
     * 同一个用户可能在多个窗口里被各计一次，直接 sum 会得到虚高的数字。
     * PV、购买数、成交额都是可加的，所以只返回这三个。
     * 需要准确的品牌 UV 时，应该从明细表 dwd_user_behavior 用 uniq 现算。
     */
    @GetMapping("/brand-ranking")
    public List<Map<String, Object>> brandRanking(@RequestParam(defaultValue = "10") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    brand,
                    sum(pv)           AS pv,
                    sum(purchase_cnt) AS purchase_cnt,
                    round(sum(gmv), 2) AS gmv
                FROM ads_realtime_brand_stats FINAL
                WHERE brand != 'unknown'
                GROUP BY brand
                ORDER BY gmv DESC
                LIMIT ?
                """, limit);
    }

    /**
     * 类目成交额排行（跨窗口汇总）。
     *
     * <p>同样不返回 UV —— 理由见 {@link #brandRanking}。
     */
    @GetMapping("/category-ranking")
    public List<Map<String, Object>> categoryRanking(@RequestParam(defaultValue = "10") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    category_id,
                    sum(pv)           AS pv,
                    sum(purchase_cnt) AS purchase_cnt,
                    round(sum(gmv), 2) AS gmv
                FROM ads_realtime_category_stats FINAL
                GROUP BY category_id
                ORDER BY gmv DESC
                LIMIT ?
                """, limit);
    }

    /**
     * 商品成交额排行 —— 直接从明细表算，因为商品维度没有实时窗口表。
     *
     * <p>用明细表而不是窗口表的另一个好处：可以真正做去重（跨窗口的 UV 不可加）。
     *
     * <p><b>UV 用 {@code uniq}（近似）而不是 {@code uniqExact}（精确）</b>：
     * 实测同一查询 uniqExact 峰值内存 1.60 GiB，远超 docs/性能压测报告.md 里定的
     * 「单查询 &lt; 500 MiB」约束；换 uniq 后降到几百 MiB、耗时也从 2.2 秒降到 1 秒。
     * 代价是约 1% 的误差 —— 对「哪个商品卖得好」这类排序场景完全够用。
     */
    @GetMapping("/product-ranking")
    public List<Map<String, Object>> productRanking(
            @RequestParam(defaultValue = "20") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    product_id,
                    any(brand)                                  AS brand,
                    countIf(event_type = 'view')                AS pv,
                    countIf(event_type = 'purchase')            AS purchase_cnt,
                    uniq(user_id)                               AS uv,
                    round(sumIf(price, event_type = 'purchase'), 2) AS gmv
                FROM dwd_user_behavior
                GROUP BY product_id
                ORDER BY gmv DESC
                LIMIT ?
                """, limit);
    }
}
