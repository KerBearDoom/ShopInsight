package com.company.shopinsight.job;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Set;

/**
 * 最小可行链路：Kafka -> Flink（解析 + 清洗）-> ClickHouse。
 *
 * <p>输入是 UserBehavior.csv 的行格式，5 个字段：
 * {@code user_id,item_id,category_id,behavior_type,timestamp}
 *
 * <p>输出是 7 个字段，对应 {@code dwd_user_behavior} 的全部列：
 * {@code user_id,item_id,category_id,behavior,event_ts,event_time,event_date}
 *
 * <p><b>为什么输出 7 列而不是 5 列：</b>表的 event_time / event_date 虽然有 DEFAULT
 * 表达式，但连接器以 {@code INSERT INTO t FORMAT CSV} 的形式写入（不带列名），
 * 这种形式 ClickHouse 要求提供全部列。所以派生列在 Flink 侧算好再发。
 *
 * <p><b>时区：</b>统一用 Asia/Shanghai。数据是淘宝的，按北京时分天才有业务含义 ——
 * 用 UTC 的话「一天」会变成北京时间早上 8 点到次日 8 点，「每日活跃」这类指标就错了。
 */
public class KafkaToClickHouseJob {

    /** 业务时区。与 ClickHouse 表 event_time 列声明的时区必须一致。 */
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final ZoneId UTC = ZoneId.of("UTC");

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(BUSINESS_ZONE);
    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(BUSINESS_ZONE);

    private static final Set<String> VALID_BEHAVIORS = Set.of("pv", "fav", "cart", "buy");

    // 数据集是 2017-11-25 ~ 2017-12-03。放宽到整个 2017 年，
    // 用来滤掉那批 0.0005% 的异常时间戳（实测存在 44016、2122867355 这类值）。
    private static final long MIN_TS = 1483228800L; // 2017-01-01 00:00:00 UTC
    private static final long MAX_TS = 1514764800L; // 2018-01-01 00:00:00 UTC

    public static void main(String[] args) throws Exception {
        String bootstrapServers = arg(args, 0, "kafka:29092");
        String topic = arg(args, 1, "user_behavior_log");
        String clickHouseUrl = arg(args, 2, "http://clickhouse:8123");
        String clickHouseUser = arg(args, 3, "shop_insight");
        String clickHousePassword = arg(args, 4, "shop_insight");
        String clickHouseDatabase = arg(args, 5, "shop_insight");
        String clickHouseTable = arg(args, 6, "dwd_user_behavior");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("shop-insight-kafka-to-clickhouse")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                clickHouseUrl, clickHouseUser, clickHousePassword,
                clickHouseDatabase, clickHouseTable);

        ClickHouseAsyncSink<String> sink = ClickHouseAsyncSink.<String>builder()
                .setElementConverter(new ClickHouseConvertor<>(String.class))
                .setClickHouseClientConfig(clientConfig)
                .setClickHouseFormat(ClickHouseFormat.CSV)
                .build();

        DataStream<String> cleaned = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .flatMap(new ParseAndClean())
                .name("parse-and-clean");

        cleaned.sinkTo(sink).name("clickhouse-sink");

        env.execute("ShopInsight Kafka->ClickHouse");
    }

    private static String arg(String[] args, int index, String defaultValue) {
        return args.length > index ? args[index] : defaultValue;
    }

    /**
     * 解析并清洗。脏数据直接丢弃（不报错、不中断作业）。
     *
     * <p>丢弃规则：
     * <ul>
     *   <li>字段数不是 5</li>
     *   <li>三个 ID 或时间戳不是纯数字</li>
     *   <li>行为类型不在 pv / fav / cart / buy 之内</li>
     *   <li>时间戳超出 2017 年</li>
     * </ul>
     */
    static class ParseAndClean implements FlatMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String raw, Collector<String> out) {
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

            Instant instant = Instant.ofEpochSecond(ts);

            out.collect(String.join(",",
                    f[0],                       // user_id
                    f[1],                       // item_id
                    f[2],                       // category_id
                    f[3],                       // behavior
                    String.valueOf(ts),         // event_ts
                    TS_FMT.format(instant),     // event_time
                    DATE_FMT.format(instant))); // event_date
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
}
