package com.company.shopinsight.job;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 一条用户行为事件（对应数据集1）。
 *
 * <p>源格式（CSV，带表头）：
 * {@code event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session}
 *
 * <p>字段声明为 public 是刻意的 —— Flink 用它来判定一个类是不是 POJO。
 * 是 POJO 就能用更高效的序列化器，否则会退化成 Kryo。
 */
public class BehaviorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 源数据的时间格式：{@code 2019-11-01 00:00:00 UTC}（后面带时区后缀） */
    private static final DateTimeFormatter SOURCE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** 事件时间，epoch 毫秒。由源字符串解析而来，用于水位线和窗口。 */
    public long eventTs;

    /** 行为类型：view / cart / purchase */
    public String eventType;

    public int productId;

    /**
     * 类目 ID。
     *
     * <p><b>必须用 long</b> —— 实际值可达 2.18e18，超出 int 范围（21 亿）。
     * 用 int 会溢出成完全错误的值。
     */
    public long categoryId;

    /** 层级类目码，如 {@code electronics.smartphone}；源数据为空时填 unknown */
    public String categoryCode;

    /** 品牌；源数据为空时填 unknown */
    public String brand;

    /** 该商品在事件发生时的价格 */
    public double price;

    public int userId;

    /** 会话 ID（UUID） */
    public String sessionId;

    public BehaviorEvent() {
    }

    /**
     * 解析源数据的时间字符串。
     *
     * <p>源格式是 {@code 2019-11-01 00:00:00 UTC}，先剥掉末尾的 {@code " UTC"}
     * 再按 UTC 解析成 epoch 毫秒。
     *
     * @return epoch 毫秒；解析失败返回 -1
     */
    public static long parseSourceTime(String raw) {
        if (raw == null) {
            return -1;
        }
        String s = raw.trim();
        if (s.endsWith(" UTC")) {
            s = s.substring(0, s.length() - 4);
        }
        try {
            return LocalDateTime.parse(s, SOURCE_TIME_FORMAT)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } catch (Exception e) {
            return -1;
        }
    }
}
