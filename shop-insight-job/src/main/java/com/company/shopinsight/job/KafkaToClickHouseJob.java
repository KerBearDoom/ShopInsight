package com.company.shopinsight.job;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
 * Kafka -> Flink（清洗 + 窗口聚合）-> ClickHouse。
 *
 * <p>一条输入流分两个出口：
 * <ul>
 *   <li><b>明细</b>：清洗后写入 {@code dwd_user_behavior}</li>
 *   <li><b>窗口指标</b>：滚动窗口聚合后写入 {@code ads_realtime_pv_uv}</li>
 * </ul>
 *
 * <p>输入格式（UserBehavior.csv 的行）：{@code user_id,item_id,category_id,behavior,timestamp}
 *
 * <p><b>时间语义</b>：用事件时间（Event Time）而不是处理时间。窗口按数据自带的时间戳
 * 划分，所以回放历史数据也能得到正确的窗口结果 —— 这是 Flink 的核心能力之一。
 */
public class KafkaToClickHouseJob {

    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(BUSINESS_ZONE);
    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(BUSINESS_ZONE);

    private static final Set<String> VALID_BEHAVIORS = Set.of("pv", "fav", "cart", "buy");

    /** 时段外的时间戳视为脏数据。数据集是 2017-11-25 ~ 12-03，放宽到整个 2017 年。 */
    private static final long MIN_TS = 1483228800L; // 2017-01-01
    private static final long MAX_TS = 1514764800L; // 2018-01-01

    public static void main(String[] args) throws Exception {
        String bootstrapServers = arg(args, 0, "kafka:29092");
        String topic = arg(args, 1, "user_behavior_log");
        String clickHouseUrl = arg(args, 2, "http://clickhouse:8123");
        String clickHouseUser = arg(args, 3, "shop_insight");
        String clickHousePassword = arg(args, 4, "shop_insight");
        String clickHouseDatabase = arg(args, 5, "shop_insight");
        String dwdTable = arg(args, 6, "dwd_user_behavior");
        String adsTable = arg(args, 7, "ads_realtime_pv_uv");
        long windowMinutes = Long.parseLong(arg(args, 8, "1"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        configureCheckpointing(env);
        configureRestartStrategy(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("shop-insight-kafka-to-clickhouse")
                // 从上次提交的 offset 接着读；首次运行（没有提交记录）才从头读。
                // 之前用 earliest() 是作业一重启就从头重读，会把数据写重。
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
                                .withTimestampAssigner((event, previousTs) -> event.eventTs * 1000L))
                .name("event-time-watermark");

        // 出口 1：明细落 dwd
        events
                .map(new ToDwdCsv())
                .name("to-dwd-csv")
                .sinkTo(clickHouseSink(clickHouseUrl, clickHouseUser, clickHousePassword,
                        clickHouseDatabase, dwdTable))
                .name("dwd-sink");

        // 出口 2：窗口聚合落 ads
        events
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(windowMinutes)))
                .aggregate(new PvUvAggregate(), new WindowToAdsCsv())
                .name("pv-uv-window")
                .sinkTo(clickHouseSink(clickHouseUrl, clickHouseUser, clickHousePassword,
                        clickHouseDatabase, adsTable))
                .name("ads-sink");

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
        // 取消作业时保留 checkpoint，否则每次改代码重跑都相当于从头开始
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    /**
     * 重启策略。
     *
     * <p>注意：Flink 2.x <b>移除了</b> {@code env.setRestartStrategy(...)}，
     * 只能通过 Configuration 配。
     */
    private static void configureRestartStrategy(StreamExecutionEnvironment env) {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.configure(conf);
    }

    private static ClickHouseAsyncSink<String> clickHouseSink(
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
    // 算子
    // -------------------------------------------------------------------------

    /**
     * 解析并清洗。脏数据直接丢弃，不报错、不中断作业。
     *
     * <p>丢弃规则：字段数不是 5 / ID 或时间戳不是数字 / 行为类型不在枚举内 /
     * 时间戳超出 2017 年。
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
            if (f.length != 5) {
                return;
            }
            for (int i = 0; i < 5; i++) {
                f[i] = f[i].trim();
            }

            if (!isDigits(f[0]) || !isDigits(f[1]) || !isDigits(f[2]) || !isDigits(f[4])) {
                return;
            }
            if (!VALID_BEHAVIORS.contains(f[3])) {
                return;
            }

            long ts;
            try {
                ts = Long.parseLong(f[4]);
            } catch (NumberFormatException e) {
                return;
            }
            if (ts < MIN_TS || ts > MAX_TS) {
                return;
            }

            try {
                out.collect(new BehaviorEvent(
                        Long.parseLong(f[0]),
                        Long.parseLong(f[1]),
                        Long.parseLong(f[2]),
                        f[3],
                        ts));
            } catch (NumberFormatException e) {
                // ID 超出 long 范围，丢弃
            }
        }

        private static boolean isDigits(String s) {
            if (s.isEmpty()) {
                return false;
            }
            for (int i = 0; i < s.length(); i++) {
                if (!Character.isDigit(s.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * 输出 dwd 表的 7 列 CSV。
     *
     * <p>为什么派生列要在这里算：连接器以 {@code INSERT INTO t FORMAT CSV} 写入
     * （不带列名），这种形式 ClickHouse 要求提供全部列，DEFAULT 列也得给。
     */
    static class ToDwdCsv implements MapFunction<BehaviorEvent, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String map(BehaviorEvent e) {
            Instant instant = Instant.ofEpochSecond(e.eventTs);
            return String.join(",",
                    String.valueOf(e.userId),
                    String.valueOf(e.itemId),
                    String.valueOf(e.categoryId),
                    e.behavior,
                    String.valueOf(e.eventTs),
                    TS_FMT.format(instant),
                    DATE_FMT.format(instant));
        }
    }

    /**
     * 窗口聚合的累加器。
     *
     * <p>口径定义：{@code pv} = 窗口内 behavior='pv' 的事件数；
     * {@code users} = 做过浏览的独立用户（即 UV）；{@code buyCnt} = 购买事件数。
     */
    public static class PvUvAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;

        public long pv;
        public long buyCnt;
        /**
         * 用 Set 存 user_id 来算 UV。
         *
         * <p>这是个权衡：单窗口内去重准，但状态会随窗口内用户数增长。
         * 用户量大时应该换成 HyperLogLog 之类的近似去重（ClickHouse 的 uniq 就是这么做的），
         * 代价是结果有约 1% 误差、换来状态大小可控。
         */
        public Set<Long> users = new HashSet<>();
    }

    /** 窗口内聚合 PV / UV / 购买数。 */
    public static class PvUvAggregate
            implements AggregateFunction<BehaviorEvent, PvUvAccumulator, PvUvAccumulator> {

        private static final long serialVersionUID = 1L;

        @Override
        public PvUvAccumulator createAccumulator() {
            return new PvUvAccumulator();
        }

        @Override
        public PvUvAccumulator add(BehaviorEvent event, PvUvAccumulator acc) {
            // PV 和 UV 只统计 behavior='pv' 的事件。
            // 这里踩过一次坑：原本写成了无条件 acc.pv++，结果窗口的 pv 等于
            // 窗口内的「总事件数」而不是浏览量，跟明细表对不上。
            if ("pv".equals(event.behavior)) {
                acc.pv++;
                acc.users.add(event.userId);
            } else if ("buy".equals(event.behavior)) {
                acc.buyCnt++;
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
            a.buyCnt += b.buyCnt;
            a.users.addAll(b.users);
            return a;
        }
    }

    /** 把窗口元信息（起止时间）和聚合结果拼成 ads 表的 CSV。 */
    public static class WindowToAdsCsv
            extends ProcessAllWindowFunction<PvUvAccumulator, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Context context,
                            Iterable<PvUvAccumulator> elements,
                            Collector<String> out) {
            PvUvAccumulator acc = elements.iterator().next();
            if (acc.pv == 0) {
                return;
            }

            TimeWindow window = context.window();
            out.collect(String.join(",",
                    TS_FMT.format(Instant.ofEpochMilli(window.getStart())),
                    TS_FMT.format(Instant.ofEpochMilli(window.getEnd())),
                    String.valueOf(acc.pv),
                    String.valueOf(acc.users.size()),
                    String.valueOf(acc.buyCnt)));
        }
    }
}
