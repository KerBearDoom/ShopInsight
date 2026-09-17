package com.company.shopinsight.job;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * Kafka → Flink（清洗 + 窗口聚合）→ ClickHouse。
 *
 * <p>数据源：数据集1（eCommerce behavior data from multi category store）
 * <pre>
 * event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
 * 2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,4d3b30da-...
 * </pre>
 *
 * <p>一条输入流分四个出口：
 * <ol>
 *   <li><b>明细</b> → {@code dwd_user_behavior}</li>
 *   <li><b>全局窗口</b> → {@code ads_realtime_pv_uv}</li>
 *   <li><b>分类目窗口</b> → {@code ads_realtime_category_stats}</li>
 *   <li><b>分品牌窗口</b> → {@code ads_realtime_brand_stats}</li>
 * </ol>
 *
 * <p>后三个必须各自独立计算，不能从全局结果派生 —— 因为 UV 是去重计数，
 * 一个用户可能访问多个类目/品牌，各部分 UV 之和 ≠ 全局 UV。
 */
public class KafkaToClickHouseJob {

    /** 源数据是 UTC，且是面向多国用户的商城，所以统一用 UTC，不转本地时区。 */
    private static final ZoneId SOURCE_ZONE = ZoneId.of("UTC");

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(SOURCE_ZONE);
    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(SOURCE_ZONE);

    private static final Set<String> VALID_EVENT_TYPES = Set.of("view", "cart", "purchase");

    /** 源数据里空的分类/品牌字段统一填这个值，让聚合不用处理 NULL。 */
    private static final String UNKNOWN = "unknown";

    private static final String DWD_TABLE = "dwd_user_behavior";
    private static final String GLOBAL_TABLE = "ads_realtime_pv_uv";
    private static final String CATEGORY_TABLE = "ads_realtime_category_stats";
    private static final String BRAND_TABLE = "ads_realtime_brand_stats";

    /** 事件时间的合理范围：源数据是 2019-10-01 ~ 2019-11-30，放宽到整个 2019 年。 */
    private static final long MIN_TS = 1546300800000L; // 2019-01-01 UTC
    private static final long MAX_TS = 1577836800000L; // 2020-01-01 UTC

    public static void main(String[] args) throws Exception {
        String bootstrapServers = arg(args, 0, "kafka:29092");
        String topic = arg(args, 1, "user_behavior_log");
        String clickHouseUrl = arg(args, 2, "http://clickhouse:8123");
        String clickHouseUser = arg(args, 3, "shop_insight");
        String clickHousePassword = arg(args, 4, "shop_insight");
        String clickHouseDatabase = arg(args, 5, "shop_insight");
        long windowMinutes = Long.parseLong(arg(args, 6, "1"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        configureCheckpointing(env);
        configureRestartStrategy(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("shop-insight-kafka-to-clickhouse")
                // 从上次提交的 offset 接着读；首次运行才从头读。
                // 用 earliest() 的话每次重启都会从头重读、把数据写重。
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 解析 + 清洗，并打上事件时间和水位线
        SingleOutputStreamOperator<BehaviorEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .flatMap(new ParseAndClean())
                .name("parse-and-clean")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<BehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, previousTs) -> event.eventTs))
                .name("event-time-watermark");

        // 出口 1：明细
        events
                .map(new ToDwdCsv())
                .name("to-dwd-csv")
                .sinkTo(sink(clickHouseUrl, clickHouseUser, clickHousePassword, clickHouseDatabase, DWD_TABLE))
                .name("dwd-sink");

        Duration windowSize = Duration.ofMinutes(windowMinutes);

        // 出口 2：全局窗口
        events
                .windowAll(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new PvUvAggregate(), new WindowToGlobalCsv())
                .name("global-window")
                .sinkTo(sink(clickHouseUrl, clickHouseUser, clickHousePassword, clickHouseDatabase, GLOBAL_TABLE))
                .name("global-sink");

        // 出口 3：分类目窗口
        events
                .keyBy(new CategoryKeySelector())
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new PvUvAggregate(), new WindowToCategoryCsv())
                .name("category-window")
                .sinkTo(sink(clickHouseUrl, clickHouseUser, clickHousePassword, clickHouseDatabase, CATEGORY_TABLE))
                .name("category-sink");

        // 出口 4：分品牌窗口 —— 品牌是数据里最接近「商家」的实体
        events
                .keyBy(new BrandKeySelector())
                .window(TumblingEventTimeWindows.of(windowSize))
                .aggregate(new PvUvAggregate(), new WindowToBrandCsv())
                .name("brand-window")
                .sinkTo(sink(clickHouseUrl, clickHouseUser, clickHousePassword, clickHouseDatabase, BRAND_TABLE))
                .name("brand-sink");

        env.execute("ShopInsight Kafka->ClickHouse");
    }

    // -------------------------------------------------------------------------
    // 环境配置
    // -------------------------------------------------------------------------

    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(5_000);
        checkpointConfig.setCheckpointTimeout(60_000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    /**
     * 重启策略。
     *
     * <p>注意：Flink 2.x 移除了 {@code env.setRestartStrategy(...)}，只能通过 Configuration 配。
     */
    private static void configureRestartStrategy(StreamExecutionEnvironment env) {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.configure(conf);
    }

    private static ClickHouseAsyncSink<String> sink(
            String url, String user, String password, String database, String table) {

        ClickHouseClientConfig clientConfig =
                new ClickHouseClientConfig(url, user, password, database, table);

        return ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setClickHouseClientConfig(clientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();
    }

    private static String arg(String[] args, int index, String defaultValue) {
        return args.length > index ? args[index] : defaultValue;
    }

    // -------------------------------------------------------------------------
    // 解析清洗
    // -------------------------------------------------------------------------

    /**
     * 解析并清洗。脏数据直接丢弃，不报错、不中断作业。
     *
     * <p>丢弃规则：字段数不是 9 / 时间解析失败 / 时间超出 2019 年 /
     * 行为类型不在 view/cart/purchase 之内。
     *
     * <p>不做丢弃但会规整的：category_code 和 brand 为空时填 {@code unknown}，
     * 因为这两个字段本身就有 32% / 14.5% 的空值，是数据的正常特征而不是脏数据。
     */
    static class ParseAndClean implements FlatMapFunction<String, BehaviorEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String raw, Collector<BehaviorEvent> out) {
            if (raw == null) {
                return;
            }
            String line = raw.trim();
            if (line.isEmpty()) {
                return;
            }

            String[] f = line.split(",");
            if (f.length != 9) {
                return;
            }
            for (int i = 0; i < 9; i++) {
                f[i] = f[i].trim();
            }

            if (!VALID_EVENT_TYPES.contains(f[1])) {
                return;
            }

            long ts = BehaviorEvent.parseSourceTime(f[0]);
            if (ts < MIN_TS || ts > MAX_TS) {
                return;
            }

            long productId;
            long categoryId;
            long userId;
            double price;
            try {
                productId = Long.parseLong(f[2]);
                categoryId = Long.parseLong(f[3]);
                price = Double.parseDouble(f[6]);
                userId = Long.parseLong(f[7]);
            } catch (NumberFormatException e) {
                return;
            }

            BehaviorEvent event = new BehaviorEvent();
            event.eventTs = ts;
            event.eventType = f[1];
            event.productId = (int) productId;
            event.categoryId = categoryId;
            event.categoryCode = f[4].isEmpty() ? UNKNOWN : f[4];
            event.brand = f[5].isEmpty() ? UNKNOWN : f[5];
            event.price = price;
            event.userId = (int) userId;
            event.sessionId = f[8];
            out.collect(event);
        }
    }

    /**
     * 输出 dwd 表的 10 列 CSV。
     *
     * <p>为什么派生列要在这里算：连接器以 {@code INSERT INTO t FORMAT CSV} 写入
     * （不带列名），这种形式 ClickHouse 要求提供全部列，DEFAULT 列也得给。
     */
    static class ToDwdCsv implements MapFunction<BehaviorEvent, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String map(BehaviorEvent e) {
            Instant instant = Instant.ofEpochMilli(e.eventTs);
            return String.join(",",
                    TS_FMT.format(instant),     // event_time
                    e.eventType,
                    String.valueOf(e.productId),
                    String.valueOf(e.categoryId),
                    e.categoryCode,
                    e.brand,
                    String.valueOf(e.price),
                    String.valueOf(e.userId),
                    e.sessionId,
                    DATE_FMT.format(instant));  // event_date
        }
    }

    // -------------------------------------------------------------------------
    // 窗口聚合
    // -------------------------------------------------------------------------

    /**
     * 窗口聚合的累加器。
     *
     * <p>口径：{@code pv} = 窗口内 view 事件数；{@code users} = 做过浏览的独立用户（UV）；
     * {@code purchaseCnt} / {@code gmv} = 购买事件数与成交额。
     */
    public static class PvUvAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;

        public long pv;
        public long purchaseCnt;
        public double gmv;
        /**
         * 用 Set 存 user_id 来算 UV。
         *
         * <p>权衡：窗口内去重精确，但状态随窗口内用户数增长。用户量大时应该换成
         * HyperLogLog 之类的近似去重，代价是约 1% 误差、换来状态大小可控。
         */
        public Set<Integer> users = new HashSet<>();

        /**
         * 窗口是否完全为空。
         *
         * <p><b>注意不能只看 pv。</b> 曾经写成 {@code if (acc.pv == 0) return;}，
         * 结果把「只有购买、没有浏览」的窗口整条丢掉了 —— 单个品牌很容易出现
         * 这种情况（用户直接从购物车下单，不经过浏览）。实测导致分品牌 GMV
         * 比全局少 3.4%，而 PV 却完全对得上，正是这个原因。
         */
        public boolean isEmpty() {
            return pv == 0 && purchaseCnt == 0;
        }
    }

    public static class PvUvAggregate
            implements AggregateFunction<BehaviorEvent, PvUvAccumulator, PvUvAccumulator> {

        private static final long serialVersionUID = 1L;

        @Override
        public PvUvAccumulator createAccumulator() {
            return new PvUvAccumulator();
        }

        @Override
        public PvUvAccumulator add(BehaviorEvent event, PvUvAccumulator acc) {
            // PV 和 UV 只统计 view 事件，不是全部事件。
            // 之前踩过坑：写成无条件自增，结果 pv 变成了窗口内的事件总数。
            switch (event.eventType) {
                case "view" -> {
                    acc.pv++;
                    acc.users.add(event.userId);
                }
                case "purchase" -> {
                    acc.purchaseCnt++;
                    acc.gmv += event.price;
                }
                default -> {
                    // cart 只影响 pv/purchase 之外的统计，当前窗口指标用不到
                }
            }
            return acc;
        }

        @Override
        public PvUvAccumulator getResult(PvUvAccumulator acc) {
            return acc;
        }

        @Override
        public PvUvAccumulator merge(PvUvAccumulator a, PvUvAccumulator b) {
            a.pv += b.pv;
            a.purchaseCnt += b.purchaseCnt;
            a.gmv += b.gmv;
            a.users.addAll(b.users);
            return a;
        }
    }

    /** 全局窗口结果 → CSV。 */
    public static class WindowToGlobalCsv
            extends ProcessAllWindowFunction<PvUvAccumulator, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Context context, Iterable<PvUvAccumulator> elements, Collector<String> out) {
            PvUvAccumulator acc = elements.iterator().next();
            if (acc.isEmpty()) {
                return;
            }
            TimeWindow window = context.window();
            out.collect(String.join(",",
                    TS_FMT.format(Instant.ofEpochMilli(window.getStart())),
                    TS_FMT.format(Instant.ofEpochMilli(window.getEnd())),
                    String.valueOf(acc.pv),
                    String.valueOf(acc.users.size()),
                    String.valueOf(acc.purchaseCnt),
                    String.valueOf(acc.gmv)));
        }
    }

    /**
     * 按类目 ID 分组。
     *
     * <p>写成显式类而不是 lambda：lambda 的返回类型可能被推断成 Object，
     * 导致 Flink 的类型提取失败、退化成 Kryo 序列化。
     */
    static class CategoryKeySelector implements KeySelector<BehaviorEvent, Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(BehaviorEvent event) {
            return event.categoryId;
        }
    }

    /** 分类目窗口结果 → CSV。 */
    public static class WindowToCategoryCsv
            extends ProcessWindowFunction<PvUvAccumulator, String, Long, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Long categoryId, Context context,
                            Iterable<PvUvAccumulator> elements, Collector<String> out) {
            PvUvAccumulator acc = elements.iterator().next();
            if (acc.isEmpty()) {
                return;
            }
            TimeWindow window = context.window();
            out.collect(String.join(",",
                    TS_FMT.format(Instant.ofEpochMilli(window.getStart())),
                    TS_FMT.format(Instant.ofEpochMilli(window.getEnd())),
                    String.valueOf(categoryId),
                    String.valueOf(acc.pv),
                    String.valueOf(acc.users.size()),
                    String.valueOf(acc.purchaseCnt),
                    String.valueOf(acc.gmv)));
        }
    }

    /** 按品牌分组。 */
    static class BrandKeySelector implements KeySelector<BehaviorEvent, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(BehaviorEvent event) {
            return event.brand;
        }
    }

    /** 分品牌窗口结果 → CSV。 */
    public static class WindowToBrandCsv
            extends ProcessWindowFunction<PvUvAccumulator, String, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(String brand, Context context,
                            Iterable<PvUvAccumulator> elements, Collector<String> out) {
            PvUvAccumulator acc = elements.iterator().next();
            if (acc.isEmpty()) {
                return;
            }
            TimeWindow window = context.window();
            out.collect(String.join(",",
                    TS_FMT.format(Instant.ofEpochMilli(window.getStart())),
                    TS_FMT.format(Instant.ofEpochMilli(window.getEnd())),
                    brand,
                    String.valueOf(acc.pv),
                    String.valueOf(acc.users.size()),
                    String.valueOf(acc.purchaseCnt),
                    String.valueOf(acc.gmv)));
        }
    }
}
