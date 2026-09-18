// 后端返回的数据模型。
//
// 注意所有的数值字段都用 `(json[...] as num)` 再转 —— 因为同一个字段
// 在不同情况下可能被序列化成 int 或 double（比如 gmv 是 Decimal，
// 而 pv 是整数计数），直接 as int / as double 会崩。

/// 实时总览的一个窗口。
class WindowMetric {
  final String windowStart;
  final int pv;
  final int uv;
  final int purchaseCnt;
  final double gmv;

  const WindowMetric({
    required this.windowStart,
    required this.pv,
    required this.uv,
    required this.purchaseCnt,
    required this.gmv,
  });

  factory WindowMetric.fromJson(Map<String, dynamic> json) => WindowMetric(
        windowStart: json['window_start'] as String? ?? '',
        pv: (json['pv'] as num?)?.toInt() ?? 0,
        uv: (json['uv'] as num?)?.toInt() ?? 0,
        purchaseCnt: (json['purchase_cnt'] as num?)?.toInt() ?? 0,
        gmv: (json['gmv'] as num?)?.toDouble() ?? 0,
      );

  /// 客单价。没有购买时返回 0，避免除零。
  double get avgOrderValue => purchaseCnt == 0 ? 0 : gmv / purchaseCnt;
}

/// 排行项。品牌、类目、商品三类排行共用 —— 字段语义一致，只是 key 不同。
class RankingItem {
  /// 排行主体的显示名（品牌名 / 类目 ID / 商品 ID）。
  final String name;
  final int pv;
  final int? uv; // 只有商品排行返回 uv
  final int purchaseCnt;
  final double gmv;

  const RankingItem({
    required this.name,
    required this.pv,
    this.uv,
    required this.purchaseCnt,
    required this.gmv,
  });

  /// 客单价。注意 uv 可能为 null，这里算的是「成交额 / 购买数」。
  double get avgOrderValue => purchaseCnt == 0 ? 0 : gmv / purchaseCnt;

  /// 从品牌排行或类目排行的 JSON 构造。
  /// [nameKey] 指定用哪个字段当显示名（brand / category_id / product_id）。
  factory RankingItem.fromJson(Map<String, dynamic> json, String nameKey) => RankingItem(
        name: '${json[nameKey] ?? '—'}',
        pv: (json['pv'] as num?)?.toInt() ?? 0,
        uv: (json['uv'] as num?)?.toInt(),
        purchaseCnt: (json['purchase_cnt'] as num?)?.toInt() ?? 0,
        gmv: (json['gmv'] as num?)?.toDouble() ?? 0,
      );
}

// ─────────────────────────────────────────────────────────────────────────
// 以下三个来自离线计算（Spark），对应「经营分析」页面
// ─────────────────────────────────────────────────────────────────────────

/// 每日转化漏斗。事件级计数，不是用户级路径。
class FunnelRow {
  final String eventDate;
  final int viewCnt;
  final int cartCnt;
  final int purchaseCnt;
  final int viewUv;
  final int purchaseUv;
  final double gmv;
  final double cartRatePct;
  final double buyRatePct;

  const FunnelRow({
    required this.eventDate,
    required this.viewCnt,
    required this.cartCnt,
    required this.purchaseCnt,
    required this.viewUv,
    required this.purchaseUv,
    required this.gmv,
    required this.cartRatePct,
    required this.buyRatePct,
  });

  factory FunnelRow.fromJson(Map<String, dynamic> j) => FunnelRow(
        eventDate: j['event_date'] as String? ?? '',
        viewCnt: (j['view_cnt'] as num?)?.toInt() ?? 0,
        cartCnt: (j['cart_cnt'] as num?)?.toInt() ?? 0,
        purchaseCnt: (j['purchase_cnt'] as num?)?.toInt() ?? 0,
        viewUv: (j['view_uv'] as num?)?.toInt() ?? 0,
        purchaseUv: (j['purchase_uv'] as num?)?.toInt() ?? 0,
        gmv: (j['gmv'] as num?)?.toDouble() ?? 0,
        cartRatePct: (j['cart_rate_pct'] as num?)?.toDouble() ?? 0,
        buyRatePct: (j['buy_rate_pct'] as num?)?.toDouble() ?? 0,
      );
}

/// 留存曲线上的一个点。retentionRate 是 0~1 的小数。
class RetentionPoint {
  final int dayOffset;
  final int retainedTotal;
  final int cohortTotal;
  final double retentionRate;

  const RetentionPoint({
    required this.dayOffset,
    required this.retainedTotal,
    required this.cohortTotal,
    required this.retentionRate,
  });

  factory RetentionPoint.fromJson(Map<String, dynamic> j) => RetentionPoint(
        dayOffset: (j['day_offset'] as num?)?.toInt() ?? 0,
        retainedTotal: (j['retained_total'] as num?)?.toInt() ?? 0,
        cohortTotal: (j['cohort_total'] as num?)?.toInt() ?? 0,
        retentionRate: (j['retention_rate'] as num?)?.toDouble() ?? 0,
      );
}

/// RFM 用户分群的一个分组。
class UserSegment {
  final String segment;
  final int users;
  final double avgRecencyDays;
  final double avgFrequency;
  final double avgMonetary;
  final double totalMonetary;

  const UserSegment({
    required this.segment,
    required this.users,
    required this.avgRecencyDays,
    required this.avgFrequency,
    required this.avgMonetary,
    required this.totalMonetary,
  });

  factory UserSegment.fromJson(Map<String, dynamic> j) => UserSegment(
        segment: j['segment'] as String? ?? '',
        users: (j['users'] as num?)?.toInt() ?? 0,
        avgRecencyDays: (j['avg_recency_days'] as num?)?.toDouble() ?? 0,
        avgFrequency: (j['avg_frequency'] as num?)?.toDouble() ?? 0,
        avgMonetary: (j['avg_monetary'] as num?)?.toDouble() ?? 0,
        totalMonetary: (j['total_monetary'] as num?)?.toDouble() ?? 0,
      );
}
