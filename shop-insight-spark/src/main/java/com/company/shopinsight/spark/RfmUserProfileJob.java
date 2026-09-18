package com.company.shopinsight.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.log1p;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.when;

/**
 * RFM 用户分群。
 *
 * <p><b>这是整个项目里最需要 Spark 的一个任务</b> —— 也是它存在的理由。
 *
 * <p>分群用的是 <b>Spark MLlib 的 KMeans 聚类</b>，不是写死的分位数阈值。
 * 分位数阈值一条 SQL 就能算，而聚类不行 —— 它要在三维空间里找自然的用户簇。
 * 这正是「为什么要保留批处理层」的答案。
 *
 * <p>R / F / M 的含义：
 * <ul>
 *   <li><b>R</b> (Recency)：最近一次行为距参考日的天数，越小越活跃</li>
 *   <li><b>F</b> (Frequency)：行为总次数</li>
 *   <li><b>M</b> (Monetary)：消费金额 —— 数据集1 有 price 字段，所以是<b>真实金额</b>，
 *       不是「用购买次数代理」的降级方案</li>
 * </ul>
 *
 * <p>产出写入 {@code ads_user_profile}。
 *
 * <p>提交：
 * <pre>
 * spark-submit --class com.company.shopinsight.spark.RfmUserProfileJob \
 *   --jars &lt;clickhouse-jdbc-all.jar&gt; target/shop-insight-spark-0.0.1-SNAPSHOT.jar
 * </pre>
 */
public class RfmUserProfileJob {

    /** 聚成几类。4 类对应「高价值 / 潜力 / 流失风险 / 一般保持」。 */
    private static final int K = 4;

    /** 固定随机种子，保证每次跑结果一致（否则每次分群编号会变）。 */
    private static final long SEED = 42L;

    public static void main(String[] args) {
        String url = args.length > 0 ? args[0] : "jdbc:clickhouse://localhost:8123/shop_insight";
        String user = args.length > 1 ? args[1] : "shop_insight";
        String password = args.length > 2 ? args[2] : "shop_insight";

        SparkSession spark = SparkSession.builder()
                .appName("ShopInsight-RFM")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        long t0 = System.currentTimeMillis();

        // ── 1. 取参考日 ─────────────────────────────────────────────────
        // 数据是历史数据（2019 年），"今天"不能用系统时间，要用数据里的最后一天。
        // 否则所有用户的 R 都会变成 2000 多天，分群失去意义。
        System.out.println("[1/5] 确定参考日…");
        String refDateSql = "SELECT toString(max(event_date)) AS d FROM dwd_user_behavior";
        Dataset<Row> refRow = read(spark, url, user, password, refDateSql);
        String refDate = refRow.collectAsList().get(0).getString(0);
        System.out.println("      参考日：" + refDate);

        // ── 2. 读用户级 R/F/M ──────────────────────────────────────────
        // 把重型聚合下推给 ClickHouse —— 它做 GROUP BY 比 Spark 快得多，
        // 而且传回来的只有 500 多万行汇总结果，不是 1.1 亿行明细。
        System.out.println("[2/5] 读取用户级 R/F/M…");
        String rfmSql = String.format("""
                SELECT
                    user_id,
                    dateDiff('day', max(event_date), toDate('%s')) AS recency_days,
                    count() AS frequency,
                    sumIf(price, event_type = 'purchase') AS monetary
                FROM dwd_user_behavior
                GROUP BY user_id
                """, refDate);
        Dataset<Row> rfm = read(spark, url, user, password, rfmSql)
                .withColumn("frequency", col("frequency").cast("double"))
                .withColumn("monetary", col("monetary").cast("double"));
        long userCount = rfm.count();
        System.out.println("      用户数：" + userCount);

        // ── 3. 分位数打分（1-5）─────────────────────────────────────────
        // 这部分用分位数就够 —— 它只是给每行一个可读的分数，
        // 真正的"分群"是下一步的聚类做的。
        System.out.println("[3/5] 计算 R/F/M 分位数得分…");
        double[] rq = rfm.stat().approxQuantile("recency_days", new double[]{0.2, 0.4, 0.6, 0.8}, 0.01);
        double[] fq = rfm.stat().approxQuantile("frequency", new double[]{0.2, 0.4, 0.6, 0.8}, 0.01);
        double[] mq = rfm.stat().approxQuantile("monetary", new double[]{0.2, 0.4, 0.6, 0.8}, 0.01);

        Dataset<Row> scored = rfm
                // R：天数越小越好，所以分档方向是反的
                .withColumn("r_score", bucketDesc("recency_days", rq))
                // F、M：越大越好
                .withColumn("f_score", bucketAsc("frequency", fq))
                .withColumn("m_score", bucketAsc("monetary", mq));

        // ── 4. KMeans 聚类 ────────────────────────────────────────────
        // 对 F 和 M 取 log1p 再聚类：这两个字段是长尾分布（少数用户消费极高），
        // 不取对数的话所有普通用户会挤成一团，聚类结果没有区分度。
        // R 本身就是有界的（0-61 天），不需要变换。
        System.out.println("[4/5] KMeans 聚类（k=" + K + "）…");
        Dataset<Row> features = scored
                .withColumn("log_f", log1p(col("frequency")))
                .withColumn("log_m", log1p(col("monetary")));

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"recency_days", "log_f", "log_m"})
                .setOutputCol("raw_features");

        // 标准化：三个维度的量纲差很多（天 vs 次数 vs 金额），
        // 不标准化的话金额会主导距离计算，R 和 F 形同虚设。
        StandardScaler scaler = new StandardScaler()
                .setInputCol("raw_features")
                .setOutputCol("features")
                .setWithMean(true)
                .setWithStd(true);

        KMeans kmeans = new KMeans()
                .setK(K)
                .setSeed(SEED)
                .setFeaturesCol("features")
                .setPredictionCol("cluster_id");

        PipelineModel model = new Pipeline()
                .setStages(new PipelineStage[]{assembler, scaler, kmeans})
                .fit(features);

        Dataset<Row> clustered = model.transform(features);

        // ── 5. 把簇编号翻译成业务名称 ───────────────────────────────────
        // KMeans 只给出 0/1/2/3 这种编号，没有业务含义。
        // 这里按每个簇的实际 R/F/M 均值排序，再映射成有意义的名称 ——
        // 关键是**每次跑都按数据重新排序**，不能假设「簇 0 就是高价值」。
        System.out.println("[5/5] 翻译簇编号并写回…");
        List<Row> stats = clustered
                .groupBy("cluster_id")
                .agg(avg("recency_days").as("avg_r"),
                     avg("frequency").as("avg_f"),
                     avg("monetary").as("avg_m"),
                     count("*").as("cnt"))
                .collectAsList();

        // 打分排序：R 越小越好（取负），F/M 越大越好
        List<Row> ranked = new ArrayList<>(stats);
        ranked.sort(Comparator.comparingDouble(r ->
                -(r.getDouble(1) * -1 + r.getDouble(2) + Math.log1p(r.getDouble(3)))));

        String[] names = {"高价值客户", "潜力客户", "一般保持", "流失风险"};
        for (int i = 0; i < ranked.size(); i++) {
            Row r = ranked.get(i);
            System.out.printf("      簇 %d → %s（R=%.0f, F=%.0f, M=%.0f, 人数=%d）%n",
                    r.getInt(0), names[Math.min(i, names.length - 1)],
                    r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getLong(4));
        }

        // 按排序结果构造 cluster_id → segment 的映射
        org.apache.spark.sql.Column segmentCol = lit(names[names.length - 1]);
        for (int i = ranked.size() - 1; i >= 0; i--) {
            segmentCol = when(col("cluster_id").equalTo(ranked.get(i).getInt(0)),
                    lit(names[Math.min(i, names.length - 1)])).otherwise(segmentCol);
        }

        Dataset<Row> result = clustered
                .withColumn("segment", segmentCol)
                .withColumn("recency_days", col("recency_days").cast("int"))
                .withColumn("frequency", col("frequency").cast("long"))
                .withColumn("monetary", round(col("monetary"), 2))
                .withColumn("cluster_id", col("cluster_id").cast("int"))
                // 用 lit() 传一个 Java Timestamp，而不是 functions.currentTimestamp() ——
                // 后者在 Spark 4.x 的 Java API 里取不到
                .withColumn("computed_at", lit(new java.sql.Timestamp(System.currentTimeMillis())))
                .select("user_id", "recency_days", "frequency", "monetary",
                        "r_score", "f_score", "m_score", "cluster_id", "segment", "computed_at");

        result.write()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("dbtable", "ads_user_profile")
                .mode("append")
                .save();

        System.out.println("完成，耗时 " + (System.currentTimeMillis() - t0) / 1000 + " 秒");
        spark.stop();
    }

    /** 通用 JDBC 读取。 */
    private static Dataset<Row> read(SparkSession spark, String url, String user,
                                     String password, String query) {
        return spark.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", user)
                .option("password", password)
                .option("query", query)
                .load();
    }

    /**
     * 越小越好的字段（R）分档：低于 20 分位 → 5 分，高于 80 分位 → 1 分。
     */
    private static org.apache.spark.sql.Column bucketDesc(String c, double[] q) {
        return when(col(c).leq(q[0]), lit(5))
                .when(col(c).leq(q[1]), lit(4))
                .when(col(c).leq(q[2]), lit(3))
                .when(col(c).leq(q[3]), lit(2))
                .otherwise(lit(1));
    }

    /**
     * 越大越好的字段（F、M）分档：高于 80 分位 → 5 分。
     */
    private static org.apache.spark.sql.Column bucketAsc(String c, double[] q) {
        return when(col(c).geq(q[3]), lit(5))
                .when(col(c).geq(q[2]), lit(4))
                .when(col(c).geq(q[1]), lit(3))
                .when(col(c).geq(q[0]), lit(2))
                .otherwise(lit(1));
    }
}
