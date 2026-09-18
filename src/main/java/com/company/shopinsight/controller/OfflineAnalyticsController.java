package com.company.shopinsight.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 离线分析接口 —— 对应展示端的「经营分析」页面。
 *
 * <p>和 {@link AnalyticsController} 的分工：
 * <ul>
 *   <li>那边是**实时**指标（秒级、窗口聚合），回答「现在发生了什么」</li>
 *   <li>这边是**离线**指标（T+1、Spark 批处理），回答「长期是什么样、该怎么做」</li>
 * </ul>
 *
 * <p>数据由 {@code shop-insight-spark} 模块的三个任务产出：
 * FunnelJob / RetentionJob / RfmUserProfileJob。
 */
@RestController
@RequestMapping("/api")
public class OfflineAnalyticsController {

    private final JdbcTemplate clickHouse;

    public OfflineAnalyticsController(JdbcTemplate clickHouseJdbcTemplate) {
        this.clickHouse = clickHouseJdbcTemplate;
    }

    /**
     * 每日转化漏斗。按日期倒序返回，前端画趋势或取最近一天看结构。
     *
     * <p>注意这里的漏斗是**事件级**的（各环节的事件计数），不是用户级路径。
     * 事件级漏斗回答「哪个环节量级掉得最厉害」，已经够商家定位问题。
     */
    @GetMapping("/funnel")
    public List<Map<String, Object>> funnel(@RequestParam(defaultValue = "30") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    toString(event_date)          AS event_date,
                    view_cnt, cart_cnt, purchase_cnt,
                    view_uv, purchase_uv,
                    round(gmv, 2)                 AS gmv,
                    round(cart_rate * 100, 3)     AS cart_rate_pct,
                    round(buy_rate * 100, 3)      AS buy_rate_pct
                FROM ads_funnel_daily FINAL
                ORDER BY event_date DESC
                LIMIT ?
                """, limit);
    }

    /**
     * 留存曲线。
     *
     * <p>返回的是**按批次大小加权**的平均留存率 —— 直接把各批次的留存率做算术平均
     * 会让小批次（比如最后几天只有几百人）和几十万人的批次等权，曲线失真。
     *
     * <pre>
     *   retention_rate = Σ retained_users / Σ cohort_users
     * </pre>
     *
     * <p><b>⚠️ 别名不能和源列同名。</b> 写成 {@code sum(retained_users) AS retained_users}
     * 会报 {@code ILLEGAL_AGGREGATION} —— ClickHouse 的别名会遮蔽同名的源列，
     * 解析器看到后面再出现 {@code retained_users} 就以为是对别名的嵌套聚合。
     * 所以这里用 {@code _total} 后缀区分。
     */
    @GetMapping("/retention")
    public List<Map<String, Object>> retention(@RequestParam(defaultValue = "30") int maxDay) {
        return clickHouse.queryForList("""
                SELECT
                    day_offset,
                    sum(retained_users)                                          AS retained_total,
                    sum(cohort_users)                                            AS cohort_total,
                    round(sum(retained_users) / sum(cohort_users), 4)            AS retention_rate
                FROM ads_retention_daily FINAL
                WHERE day_offset <= ?
                GROUP BY day_offset
                ORDER BY day_offset
                """, maxDay);
    }

    /**
     * RFM 用户分群汇总。
     *
     * <p>这是「经营分析」页最有价值的一块 —— 实测 8.2% 的高价值客户贡献了
     * 76% 的成交额，商家看到这个立刻知道营销预算该花在哪。
     */
    @GetMapping("/user-segments")
    public List<Map<String, Object>> userSegments() {
        return clickHouse.queryForList("""
                SELECT
                    segment,
                    count()                          AS users,
                    round(avg(recency_days), 1)      AS avg_recency_days,
                    round(avg(frequency), 1)         AS avg_frequency,
                    round(avg(monetary), 2)          AS avg_monetary,
                    round(sum(monetary), 2)          AS total_monetary
                FROM ads_user_profile FINAL
                GROUP BY segment
                ORDER BY total_monetary DESC
                """);
    }

    /**
     * 用户分群占比 —— 给饼图/条形图用的精简版（只要人数和汇总金额）。
     */
    @GetMapping("/user-segments/summary")
    public Map<String, Object> userSegmentsSummary() {
        return clickHouse.queryForMap("""
                SELECT
                    count()                          AS total_users,
                    round(sum(monetary), 2)          AS total_monetary,
                    countIf(segment = '高价值客户')   AS high_value_users,
                    round(sumIf(monetary, segment = '高价值客户'), 2) AS high_value_monetary
                FROM ads_user_profile FINAL
                """);
    }
}
