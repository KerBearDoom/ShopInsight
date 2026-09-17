package com.company.shopinsight.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 埋点数据生产者：读 UserBehavior.csv，按指定速率发到 Kafka。
 *
 * <p>这是「采集层」的补位 —— 真实场景是 App 埋点 SDK 实时上报，
 * 这里用离线数据集回放来模拟持续的数据流。
 *
 * <p>跑在<strong>宿主机</strong>上（不是容器里），所以默认连 {@code localhost:9092}，
 * 走的是 Kafka 的 EXTERNAL 监听器。
 *
 * <p>用法：
 * <pre>
 * java -cp target/shop-insight-job-0.0.1-SNAPSHOT.jar \
 *      com.company.shopinsight.producer.BehaviorLogProducer \
 *      &lt;csv路径&gt; [bootstrap] [topic] [速率条/秒] [最多发多少条]
 * </pre>
 *
 * <p>速率用固定值而不是按时间戳回放。数据集的 9 天跨度过长，按时间戳回放要么
 * 慢得没法演示、要么因为异常时间戳卡住。固定速率更可控。
 */
public class BehaviorLogProducer {

    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    /**
     * 排序批次大小。攒够这么多条就按时间戳排一次序再发。
     *
     * <p>为什么必须排序：Flink 的窗口按<strong>事件时间</strong>划分，
     * 水位线一旦推过窗口的结束时间，之后到达的、时间戳更早的记录会被当作
     * 「迟到数据」直接丢弃（默认 allowedLateness 为 0）。
     *
     * <p>而 UserBehavior.csv 是离线 dump，时间戳<strong>不是</strong>有序的 ——
     * 实测前 30 万行跨了 2017-09-11 到 12-03。直接顺序发送的话，
     * 窗口只能统计到约 3% 的数据，其余全被丢掉。
     *
     * <p>真实场景里事件是按发生顺序到达的，所以排序反而<strong>更贴近真实的流</strong>。
     *
     * <p>批次大小是个权衡：<strong>批次之间仍可能乱序</strong>（第 N+1 批的头几条
     * 可能比第 N 批的尾条更早），所以批次越大、被丢弃的迟到数据越少，代价是内存。
     * 30 万条约占用几十 MB。要完全有序就得先把整个数据集排序落盘。
     */
    private static final int SORT_BATCH_SIZE = 300_000;

    /** 固定发到 0 号分区，保证流是有序的。理由见 sendBatch 的注释。 */
    private static final int PRODUCER_PARTITION = 0;

    /** 按第 5 列（时间戳）升序。 */
    private static final Comparator<String> BY_TIMESTAMP = (a, b) -> {
        long ta = timestampOf(a);
        long tb = timestampOf(b);
        return Long.compare(ta, tb);
    };

    public static void main(String[] args) throws Exception {
        String csvPath = args.length > 0 ? args[0] : "/Users/kerbear/Desktop/Project_IV/UserBehavior.csv";
        String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        String topic = args.length > 2 ? args[2] : "user_behavior_log";
        int ratePerSecond = args.length > 3 ? Integer.parseInt(args[3]) : 2000;
        long maxRecords = args.length > 4 ? Long.parseLong(args[4]) : Long.MAX_VALUE;

        Path path = Paths.get(csvPath);
        if (!Files.exists(path)) {
            System.err.println("找不到文件：" + path.toAbsolutePath());
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 吞吐优先：攒批再发，避免每条一个请求
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到停止信号，正在收尾...");
            RUNNING.set(false);
        }));

        System.out.printf("生产者启动%n  CSV     : %s%n  Kafka   : %s%n  Topic   : %s%n  速率    : %,d 条/秒%n  上限    : %s%n%n",
                path, bootstrapServers, topic, ratePerSecond,
                maxRecords == Long.MAX_VALUE ? "不限" : String.format("%,d", maxRecords));

        long sent = 0;
        long skipped = 0;
        long startedAt = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {

            String line;
            List<String> batch = new ArrayList<>(SORT_BATCH_SIZE);

            while (RUNNING.get() && sent < maxRecords && (line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                // 只发合法行。脏数据留给 Flink 清洗层处理是另一种选择，
                // 但那样测不出清洗规则 —— 那里需要的是「人为构造的脏数据」，
                // 而不是数据集自带的这 0.0005%。
                if (!looksValid(line)) {
                    skipped++;
                    continue;
                }

                batch.add(line);

                if (batch.size() >= SORT_BATCH_SIZE) {
                    sent += sendBatch(producer, topic, batch);
                    batch.clear();
                    pace(sent, startedAt, ratePerSecond);
                    long elapsedSec = Math.max(1, (System.currentTimeMillis() - startedAt) / 1000);
                    System.out.printf("  已发送 %,d 条（跳过 %,d），平均 %,d 条/秒%n",
                            sent, skipped, sent / elapsedSec);
                }
            }

            if (!batch.isEmpty() && sent < maxRecords) {
                sent += sendBatch(producer, topic, batch);
            }
            producer.flush();
        }

        long elapsedSec = Math.max(1, (System.currentTimeMillis() - startedAt) / 1000);
        System.out.printf("%n发送结束%n  总计 %,.0f 条，跳过 %,d 条，耗时 %d 秒，平均 %,d 条/秒%n",
                (double) sent, skipped, elapsedSec, sent / elapsedSec);
    }

    /**
     * 按时间戳排序后发送一批，返回发送条数。
     *
     * <p><b>全部发到 0 号分区</b>（而不是让 Kafka 轮询分发）。原因：
     * topic 有 3 个分区时，Flink 会并发读这 3 个分区，各自的读取进度会错开 ——
     * 一个分区读到 12 月、另一个可能还在 11 月。水位线按最快的前进，
     * 落后分区里时间戳更早的记录就全被当成迟到数据丢弃。
     *
     * <p>实测：轮询分发到 3 个分区时，窗口只能统计到约 47% 的数据；
     * 全部走一个分区后，单分区内部严格有序，不再丢。
     *
     * <p>真实场景里各分区是持续、同步地被写入的，不会出现这种跨分区错位 ——
     * 这是「把 83 天数据压缩到 37 秒回放」这个模拟方式带来的问题。
     * 生产环境若要并行，应按 key（比如 user_id）分区，保证同一个 key 的事件有序。
     */
    private static int sendBatch(KafkaProducer<String, String> producer, String topic, List<String> batch) {
        batch.sort(BY_TIMESTAMP);
        for (String record : batch) {
            producer.send(new ProducerRecord<>(topic, PRODUCER_PARTITION, null, record));
        }
        return batch.size();
    }

    private static long timestampOf(String line) {
        int lastComma = line.lastIndexOf(',');
        try {
            return Long.parseLong(line.substring(lastComma + 1).trim());
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE; // 解析不了的排到最后
        }
    }

    /** 粗略过滤：5 个字段、三个 ID 和时间戳是数字。 */
    private static boolean looksValid(String line) {
        String[] f = line.split(",");
        if (f.length != 5) {
            return false;
        }
        return isDigits(f[0]) && isDigits(f[1]) && isDigits(f[2]) && isDigits(f[4]);
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

    /**
     * 速率控制：如果发得太快就等一下。
     * 每 10000 条调一次，避免每条都算时间。
     */
    private static void pace(long sent, long startedAt, int ratePerSecond) throws InterruptedException {
        long targetMs = sent * 1000L / ratePerSecond;
        long actualMs = System.currentTimeMillis() - startedAt;
        long sleepMs = targetMs - actualMs;
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
    }
}
