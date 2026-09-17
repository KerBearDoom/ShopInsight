package com.company.shopinsight.controller;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 数据分析接口。数据全部来自 ClickHouse。
 *
 * <p>这是最简版，先打通「浏览器 → Spring Boot → ClickHouse」的链路。
 * 后续按 docs/指标定义表.md 的 7 个页面逐步补齐接口。
 */
@RestController
@RequestMapping("/api")
public class AnalyticsController {

    private final JdbcTemplate clickHouse;

    public AnalyticsController(JdbcTemplate clickHouseJdbcTemplate) {
        this.clickHouse = clickHouseJdbcTemplate;
    }

    /**
     * 实时总览：最近 N 个窗口的全局 PV / UV / 购买数。
     *
     * <p>表是 ReplacingMergeTree，去重是后台异步的，所以用
     * GROUP BY + max() 保证即使有未合并的重复行，结果也正确。
     */
    @GetMapping("/overview")
    public List<Map<String, Object>> overview(
            @RequestParam(defaultValue = "30") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    formatDateTime(window_start, '%Y-%m-%d %H:%i:%S', 'Asia/Shanghai') AS window_start,
                    max(pv)      AS pv,
                    max(uv)      AS uv,
                    max(buy_cnt) AS buy_cnt
                FROM ads_realtime_pv_uv
                GROUP BY window_start
                ORDER BY window_start DESC
                LIMIT ?
                """, limit);
    }

    /**
     * 类目实时统计：最近窗口内各类目的 PV / UV / 购买数。
     */
    @GetMapping("/category-stats")
    public List<Map<String, Object>> categoryStats(
            @RequestParam(defaultValue = "20") int limit) {
        return clickHouse.queryForList("""
                SELECT
                    formatDateTime(window_start, '%Y-%m-%d %H:%i:%S', 'Asia/Shanghai') AS window_start,
                    category_id,
                    max(pv)      AS pv,
                    max(uv)      AS uv,
                    max(buy_cnt) AS buy_cnt
                FROM ads_realtime_category_stats
                GROUP BY window_start, category_id
                ORDER BY window_start DESC, pv DESC
                LIMIT ?
                """, limit);
    }
}
