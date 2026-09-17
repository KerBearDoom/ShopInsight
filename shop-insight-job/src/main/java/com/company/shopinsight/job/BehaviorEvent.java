package com.company.shopinsight.job;

import java.io.Serializable;

/**
 * 一条用户行为事件。
 *
 * <p>字段声明为 public 是刻意的 —— Flink 用它来判定一个类是不是 POJO。
 * 如果是 POJO，Flink 能用更高效的序列化器；否则会退化成 Kryo。
 */
public class BehaviorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public long userId;
    public long itemId;
    public long categoryId;
    public String behavior;
    /** Unix 秒级时间戳，来自原始数据。 */
    public long eventTs;

    /** Flink POJO 要求有无参构造。 */
    public BehaviorEvent() {
    }

    public BehaviorEvent(long userId, long itemId, long categoryId, String behavior, long eventTs) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.eventTs = eventTs;
    }
}
