package com.company.shopinsight.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;

/**
 * 留存分析（按首次活跃日期分批次）。
 *
 * <p>回答的问题是：<b>「用户第一次来之后，第 N 天还回不回来？」</b>
 *
 * <p>产出写入 {@code ads_retention_daily}，形状是「批次 × 第 N 天」的矩阵。
 *
 * <p><b>为什么必须离线算：</b>实时窗口只保留当前窗口的累加器，算不了
 * 「跨 N 天的用户回访」—— 这需要把整段时间的用户活跃矩阵放在一起看。
 *
 * <p><b>计算分工：</b>
 * <ul>
 *   <li>ClickHouse 负责 {@code SELECT DISTINCT user_id, event_date} ——
 *       它做去重比 Spark 快得多，1500 万行的结果也比 1.1 亿行明细轻得多</li>
 *   <li>Spark 负责 cohort 关联和矩阵聚合 —— join + groupBy 是 shuffle 密集型操作，
 *       正是 Spark 擅长的</li>
 * </ul>
 *
 * <p>提交：
 * <pre>
 * spark-submit --class com.company.shopinsight.spark.RetentionJob \
 *   --jars &lt;clickhouse-jdbc-all.jar&gt; target/shop-insight-spark-0.0.1-SNAPSHOT.jar
 * </pre>
 */
public class RetentionJob {

    public static void main(String[] args) {
        String url = args.length > 0 ? args[0] : "jdbc:clickhouse://localhost:8123/shop_insight";
        String user = args.length > 1 ? args[1] : "shop_insight";
        String password = args.length > 2 ? args[2] : "shop_insight";

        SparkSession spark = SparkSession.builder()
                .appName("ShopInsight-Retention")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        long t0 = System.currentTimeMillis();

        // ── 1. 读用户-日期对 ────────────────────────────────────────────
        // 先在 ClickHouse 里 DISTINCT 掉同一用户同一天的多条记录，
        // 否则「活跃」会被重复计数，留存率会虚高。
        System.out.println("[1/4] 读取用户-日期对（ClickHouse 端已去重）…");
        Dataset<Row> activity = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("query",
                        "SELECT DISTINCT user_id, event_date FROM dwd_user_behavior")
                .load()
                // JDBC 读回来可能带时区，统一成日期再比较，避免 datediff 差一天
                .withColumn("event_date", col("event_date").cast("date"));
        long pairs = activity.count();
        System.out.println("      用户-日期对数：" + pairs);

        // ── 2. 计算每个用户的批次日期 ───────────────────────────────────
        // 批次 = 用户在整个数据跨度里**第一次**出现的日期。
        // 这是留存分析的标准定义（首次活跃归因）。
        System.out.println("[2/4] 计算每个用户的首次活跃日期…");
        Dataset<Row> cohort = activity
                .groupBy("user_id")
                .agg(min("event_date").as("cohort_date"));

        // ── 3. 关联并计算第 N 天 ────────────────────────────────────────
        System.out.println("[3/4] 关联并计算 day_offset…");
        Dataset<Row> withOffset = activity
                .join(cohort, "user_id")
                .withColumn("day_offset", datediff(col("event_date"), col("cohort_date")))
                // 只保留 30 天内 —— 再往后的留存数字噪声大，而且卡片上也放不下
                .filter(col("day_offset").leq(30));

        Dataset<Row> retained = withOffset
                .groupBy("cohort_date", "day_offset")
                .agg(countDistinct("user_id").as("retained_users"));

        // 每个批次的用户总数 = 该批次第 0 天的活跃数（按定义）
        Dataset<Row> cohortSize = cohort
                .groupBy("cohort_date")
                .agg(count("*").as("cohort_users"));

        Dataset<Row> result = retained
                .join(cohortSize, "cohort_date")
                .withColumn("retention_rate",
                        round(col("retained_users").divide(col("cohort_users")), 6))
                .withColumn("day_offset", col("day_offset").cast("int"))
                .withColumn("cohort_users", col("cohort_users").cast("int"))
                .withColumn("retained_users", col("retained_users").cast("int"))
                .select("cohort_date", "day_offset", "cohort_users", "retained_users", "retention_rate")
                .orderBy("cohort_date", "day_offset");

        // ── 4. 写回 ────────────────────────────────────────────────────
        System.out.println("[4/4] 写回 ClickHouse…");
        result.write()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("dbtable", "ads_retention_daily")
                .mode("append")
                .save();

        System.out.println("完成，耗时 " + (System.currentTimeMillis() - t0) / 1000 + " 秒");
        spark.stop();
    }
}
