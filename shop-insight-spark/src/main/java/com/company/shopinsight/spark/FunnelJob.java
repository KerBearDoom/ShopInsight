package com.company.shopinsight.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * 转化漏斗：按天统计 view → cart → purchase 各环节的转化。
 *
 * <p>产出写入 ClickHouse 的 {@code ads_funnel_daily}。
 *
 * <p>提交方式：
 * <pre>
 * spark-submit --class com.company.shopinsight.spark.FunnelJob \
 *   --jars ~/.m2/.../clickhouse-jdbc-0.10.0-all.jar \
 *   target/shop-insight-spark-0.0.1-SNAPSHOT.jar
 * </pre>
 *
 * <p><b>⚠️ 关于「这个指标该不该用 Spark」</b>：
 * 这个漏斗本质是一个 {@code GROUP BY event_date} 的简单聚合，
 * <b>ClickHouse 一条 SQL 就能算，而且更快</b>（实测同类查询 2 秒内）。
 *
 * <p>用 Spark 的理由是架构上的 —— 项目是 Lambda 架构，批处理层需要有实际产出，
 * 而且后续的留存、RFM 分群需要跨天、跨用户的多次计算，那些才是 Spark 真正的主场。
 *
 * <p>如果被问到「为什么要用 Spark 算这个」，诚实的答案是：
 * <b>这个指标不需要，但保留批处理层是为了跑 Spark 才能做的计算（如 MLlib）</b>。
 */
public class FunnelJob {

    public static void main(String[] args) {
        // 参数：0=ClickHouse URL  1=用户  2=密码  3=起始日期(可选)  4=结束日期(可选)
        String url = args.length > 0 ? args[0] : "jdbc:clickhouse://localhost:8123/shop_insight";
        String user = args.length > 1 ? args[1] : "shop_insight";
        String password = args.length > 2 ? args[2] : "shop_insight";
        String startDate = args.length > 3 ? args[3] : null;
        String endDate = args.length > 4 ? args[4] : null;

        SparkSession spark = SparkSession.builder()
                .appName("ShopInsight-Funnel")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        long t0 = System.currentTimeMillis();

        // 日期范围可选。指定了就只算这段 —— 既能小范围验证，也是增量重跑的正常做法。
        // 让 ClickHouse 在读取端就过滤掉不需要的分区，而不是全量拉过来再筛。
        StringBuilder q = new StringBuilder(
                "SELECT event_date, event_type, user_id, price FROM dwd_user_behavior");
        if (startDate != null) {
            q.append(" WHERE event_date >= '").append(startDate).append('\'');
            if (endDate != null) {
                q.append(" AND event_date <= '").append(endDate).append('\'');
            }
        }

        System.out.println("[1/3] 从 ClickHouse 读取明细…");
        System.out.println("      范围：" + (startDate == null ? "全部" : startDate + " ~ " + endDate));
        // 只读需要的四个列 —— 明细表有 10 列，少读一列就少传一份数据
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("query", q.toString())
                .load();
        System.out.println("      读取行数：" + df.count());

        System.out.println("[2/3] 聚合计算…");
        Dataset<Row> funnel = df.groupBy("event_date").agg(
                count(when(col("event_type").equalTo("view"), 1)).as("view_cnt"),
                count(when(col("event_type").equalTo("cart"), 1)).as("cart_cnt"),
                count(when(col("event_type").equalTo("purchase"), 1)).as("purchase_cnt"),
                countDistinct(when(col("event_type").equalTo("view"), col("user_id"))).as("view_uv"),
                countDistinct(when(col("event_type").equalTo("cart"), col("user_id"))).as("cart_uv"),
                countDistinct(when(col("event_type").equalTo("purchase"), col("user_id"))).as("purchase_uv"),
                sum(when(col("event_type").equalTo("purchase"), col("price")).otherwise(0)).as("gmv")
        );

        // 转化率。view_cnt 为 0 时返回 null 而不是除零崩掉
        Dataset<Row> result = funnel
                .withColumn("cart_rate",
                        when(col("view_cnt").gt(0), round(col("cart_cnt").divide(col("view_cnt")), 6)))
                .withColumn("buy_rate",
                        when(col("view_cnt").gt(0), round(col("purchase_cnt").divide(col("view_cnt")), 6)))
                // 按表定义的列顺序排列 —— JDBC 写入是按位置对应列的
                .select("event_date", "view_cnt", "cart_cnt", "purchase_cnt",
                        "view_uv", "cart_uv", "purchase_uv", "gmv", "cart_rate", "buy_rate")
                .orderBy("event_date");

        System.out.println("[3/3] 写回 ClickHouse…");
        result.write()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("dbtable", "ads_funnel_daily")
                // 表是 ReplacingMergeTree 且排序键是 event_date，
                // 所以重跑时同一天的行会替换而不是追加
                .mode("append")
                .save();

        long cost = (System.currentTimeMillis() - t0) / 1000;
        System.out.println("完成，耗时 " + cost + " 秒");

        spark.stop();
    }
}
