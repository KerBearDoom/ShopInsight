import 'dart:async';

import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../api/analytics_api.dart';
import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../utils/format.dart';

/// 经营分析页 —— 展示 Spark 离线算出来的三个指标。
///
/// 和「实时总览」页的分工：
///   实时总览：秒级窗口，回答「现在发生了什么」
///   经营分析：T+1 批处理，回答「长期是什么样、下一步该怎么做」
///
/// 数据刷新频率比实时页低得多（1 分钟一次就够了）—— 因为这些是 T+1 指标，
/// 每天才更新一次。
class InsightPage extends StatefulWidget {
  const InsightPage({super.key});

  @override
  State<InsightPage> createState() => _InsightPageState();
}

class _InsightPageState extends State<InsightPage> {
  static const _refreshInterval = Duration(minutes: 1);

  List<FunnelRow> _funnel = const [];
  List<UserSegment> _segments = const [];
  List<RetentionPoint> _retention = const [];
  String? _error;
  Timer? _timer;

  @override
  void initState() {
    super.initState();
    _load();
    _timer = Timer.periodic(_refreshInterval, (_) => _load());
  }

  @override
  void dispose() {
    _timer?.cancel();
    super.dispose();
  }

  Future<void> _load() async {
    try {
      // 三个接口并发拉，别串行等
      final results = await Future.wait([
        AnalyticsApi.funnel(limit: 30),
        AnalyticsApi.userSegments(),
        AnalyticsApi.retention(maxDay: 30),
      ]);
      if (!mounted) return;
      setState(() {
        _funnel = results[0] as List<FunnelRow>;
        _segments = results[1] as List<UserSegment>;
        _retention = results[2] as List<RetentionPoint>;
        _error = null;
      });
    } catch (e) {
      if (!mounted) return;
      setState(() => _error = '$e');
    }
  }

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    return RefreshIndicator(
      onRefresh: _load,
      child: ListView(
        padding: const EdgeInsets.all(20),
        children: [
          Text('经营分析',
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.w600, color: p.ink)),
          const SizedBox(height: 4),
          Text('由 Spark 离线计算，T+1 更新 —— 回答「长期趋势和该怎么做」',
              style: TextStyle(fontSize: 12.5, color: p.ink2)),

          if (_error != null)
            Padding(
              padding: const EdgeInsets.symmetric(vertical: 24),
              child: Text('取数失败：$_error',
                  style: const TextStyle(color: Color(0xFFD03B3B), fontSize: 13)),
            )
          else ...[
            const SizedBox(height: 18),
            _FunnelCard(rows: _funnel),
            const SizedBox(height: 18),
            _SegmentCard(segments: _segments),
            const SizedBox(height: 18),
            _RetentionCard(points: _retention),
          ],
        ],
      ),
    );
  }
}

// ── 通用卡片外壳 ────────────────────────────────────────────────────────
class _Section extends StatelessWidget {
  final String title;
  final String desc;
  final Widget child;
  const _Section({required this.title, required this.desc, required this.child});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    return Card(
      child: Padding(
        padding: const EdgeInsets.fromLTRB(20, 18, 20, 20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(title,
                style: TextStyle(fontSize: 14, fontWeight: FontWeight.w600, color: p.ink)),
            const SizedBox(height: 2),
            Text(desc, style: TextStyle(fontSize: 12.5, color: p.ink2)),
            const SizedBox(height: 16),
            child,
          ],
        ),
      ),
    );
  }
}

// ── 转化漏斗 ────────────────────────────────────────────────────────────
class _FunnelCard extends StatelessWidget {
  final List<FunnelRow> rows;
  const _FunnelCard({required this.rows});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    if (rows.isEmpty) {
      return const _Section(title: '转化漏斗', desc: '', child: Text('暂无数据'));
    }
    // 取最近一天
    final r = rows.first;

    final stages = [
      ('浏览', r.viewCnt, 1.0),
      ('加购', r.cartCnt, r.viewCnt == 0 ? 0.0 : r.cartCnt / r.viewCnt),
      ('购买', r.purchaseCnt, r.viewCnt == 0 ? 0.0 : r.purchaseCnt / r.viewCnt),
    ];

    return _Section(
      title: '转化漏斗',
      desc: '${r.eventDate} · 定位流失环节',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          for (var i = 0; i < stages.length; i++) ...[
            _FunnelBar(
              label: stages[i].$1,
              count: stages[i].$2,
              ratio: stages[i].$3,
              color: p.series1,
            ),
            if (i < stages.length - 1)
              Padding(
                padding: const EdgeInsets.symmetric(vertical: 6),
                child: Row(children: [
                  const SizedBox(width: 56),
                  Icon(Icons.arrow_downward, size: 13, color: p.muted),
                  const SizedBox(width: 6),
                  Text(
                    '流失 ${(100 - stages[i + 1].$3 * 100).toStringAsFixed(1)}%',
                    style: TextStyle(fontSize: 11.5, color: p.muted),
                  ),
                ]),
              ),
          ],
        ],
      ),
    );
  }
}

class _FunnelBar extends StatelessWidget {
  final String label;
  final int count;
  final double ratio; // 0~1，相对第一环节
  final Color color;

  const _FunnelBar({
    required this.label,
    required this.count,
    required this.ratio,
    required this.color,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    return Row(
      crossAxisAlignment: CrossAxisAlignment.center,
      children: [
        SizedBox(
          width: 56,
          child: Text(label, style: TextStyle(fontSize: 13, color: p.ink)),
        ),
        Expanded(
          child: LayoutBuilder(builder: (context, c) {
            // 条形宽度按真实比例，但留一个最小可见宽度 ——
            // 1.6% 在真实比例下几乎看不见，那就失去了「看到量级差」的意义。
            // 真实比例靠右侧的百分比数字精确表达，条形负责传达「差多少」的直觉。
            final w = (c.maxWidth * ratio).clamp(3.0, c.maxWidth);
            return Align(
              alignment: Alignment.centerLeft,
              child: Container(
                width: w,
                height: 26,
                decoration: BoxDecoration(
                  color: color.withValues(alpha: 0.85),
                  borderRadius: const BorderRadius.only(
                    topRight: Radius.circular(4),
                    bottomRight: Radius.circular(4),
                  ),
                ),
              ),
            );
          }),
        ),
        const SizedBox(width: 12),
        SizedBox(
          width: 96,
          child: Text(
            full(count),
            textAlign: TextAlign.right,
            style: TextStyle(
              fontSize: 13,
              color: p.ink,
              fontFeatures: const [FontFeature.tabularFigures()],
            ),
          ),
        ),
        SizedBox(
          width: 58,
          child: Text(
            '${(ratio * 100).toStringAsFixed(2)}%',
            textAlign: TextAlign.right,
            style: TextStyle(
              fontSize: 12.5,
              color: p.ink2,
              fontFeatures: const [FontFeature.tabularFigures()],
            ),
          ),
        ),
      ],
    );
  }
}

// ── RFM 用户分群 ────────────────────────────────────────────────────────
class _SegmentCard extends StatelessWidget {
  final List<UserSegment> segments;
  const _SegmentCard({required this.segments});

  /// 分群配色。用状态色而不是序列色 —— 这四个是「分群」不是「序列」，
  /// 而且高价值/流失风险本身带有好坏语义。
  static const _colors = {
    '高价值客户': Color(0xFF0CA30C),
    '潜力客户': Color(0xFF2A78D6),
    '一般保持': Color(0xFFEDA100),
    '流失风险': Color(0xFFD03B3B),
  };

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    if (segments.isEmpty) {
      return const _Section(title: '用户分群', desc: '', child: Text('暂无数据'));
    }

    final totalUsers = segments.fold<int>(0, (s, e) => s + e.users);
    final totalGmv = segments.fold<double>(0, (s, e) => s + e.totalMonetary);
    final highValue = segments.firstWhere(
      (s) => s.segment == '高价值客户',
      orElse: () => segments.first,
    );
    final hvGmvShare = totalGmv == 0 ? 0.0 : highValue.totalMonetary / totalGmv;

    return _Section(
      title: '用户分群（RFM）',
      desc: 'Spark MLlib KMeans 聚类 · 共 ${full(totalUsers)} 个用户',
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // 一句话结论 —— 这是整页最有商业价值的一句话
          Container(
            width: double.infinity,
            padding: const EdgeInsets.all(12),
            decoration: BoxDecoration(
              color: p.series1.withValues(alpha: 0.08),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Text(
              '${(highValue.users / totalUsers * 100).toStringAsFixed(1)}% 的用户'
              '（${full(highValue.users)} 人）贡献了 '
              '${(hvGmvShare * 100).toStringAsFixed(1)}% 的成交额',
              style: TextStyle(fontSize: 13, color: p.ink, height: 1.5),
            ),
          ),
          const SizedBox(height: 16),
          for (final s in segments) ...[
            Row(children: [
              Container(
                width: 9,
                height: 9,
                decoration: BoxDecoration(
                  color: _colors[s.segment] ?? p.muted,
                  borderRadius: BorderRadius.circular(2),
                ),
              ),
              const SizedBox(width: 8),
              SizedBox(
                width: 76,
                child: Text(s.segment, style: TextStyle(fontSize: 13, color: p.ink)),
              ),
              Expanded(
                child: Text(
                  '${full(s.users)} 人 · ${(s.users / totalUsers * 100).toStringAsFixed(1)}%',
                  style: TextStyle(
                    fontSize: 12.5,
                    color: p.ink2,
                    fontFeatures: const [FontFeature.tabularFigures()],
                  ),
                ),
              ),
              Text(
                '${money(s.totalMonetary)} · 人均 ${money2(s.avgMonetary)}',
                style: TextStyle(
                  fontSize: 12.5,
                  color: p.ink2,
                  fontFeatures: const [FontFeature.tabularFigures()],
                ),
              ),
            ]),
            const SizedBox(height: 10),
          ],
        ],
      ),
    );
  }
}

// ── 留存曲线 ────────────────────────────────────────────────────────────
class _RetentionCard extends StatelessWidget {
  final List<RetentionPoint> points;
  const _RetentionCard({required this.points});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    if (points.isEmpty) {
      return const _Section(title: '留存曲线', desc: '', child: Text('暂无数据'));
    }

    final d1 = points.firstWhere((e) => e.dayOffset == 1, orElse: () => points.last);

    return _Section(
      title: '留存曲线',
      desc: '按首次活跃日期分批 · 次日留存 '
          '${(d1.retentionRate * 100).toStringAsFixed(1)}%',
      child: SizedBox(
        height: 200,
        child: LineChart(
          LineChartData(
            minX: 0,
            maxX: points.last.dayOffset.toDouble(),
            minY: 0,
            maxY: 100,
            gridData: FlGridData(
              show: true,
              drawVerticalLine: false,
              horizontalInterval: 25,
              getDrawingHorizontalLine: (_) =>
                  FlLine(color: p.grid, strokeWidth: 1),
            ),
            borderData: FlBorderData(show: false),
            titlesData: FlTitlesData(
              topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
              rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
              leftTitles: AxisTitles(
                sideTitles: SideTitles(
                  showTitles: true,
                  reservedSize: 40,
                  interval: 25,
                  getTitlesWidget: (v, m) => Padding(
                    padding: const EdgeInsets.only(right: 6),
                    child: Text('${v.toInt()}%',
                        style: TextStyle(fontSize: 11, color: p.muted),
                        textAlign: TextAlign.right),
                  ),
                ),
              ),
              bottomTitles: AxisTitles(
                sideTitles: SideTitles(
                  showTitles: true,
                  reservedSize: 24,
                  interval: 5,
                  getTitlesWidget: (v, m) => Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text('D${v.toInt()}',
                        style: TextStyle(fontSize: 11, color: p.muted)),
                  ),
                ),
              ),
            ),
            lineTouchData: LineTouchData(
              touchTooltipData: LineTouchTooltipData(
                getTooltipColor: (_) => p.surface,
                getTooltipItems: (touched) => touched.map((spot) {
                  return LineTooltipItem(
                    '第 ${spot.x.toInt()} 天\n',
                    TextStyle(fontSize: 11, color: p.ink2),
                    children: [
                      TextSpan(
                        text: '${spot.y.toStringAsFixed(1)}%',
                        style: TextStyle(
                            fontSize: 13, color: p.ink, fontWeight: FontWeight.w600),
                      ),
                    ],
                  );
                }).toList(),
              ),
            ),
            lineBarsData: [
              LineChartBarData(
                spots: [
                  for (final e in points)
                    FlSpot(e.dayOffset.toDouble(), e.retentionRate * 100),
                ],
                isCurved: false,
                color: p.series1,
                barWidth: 2,
                isStrokeCapRound: true,
                // 点太密，不画点；末端单独标一个
                dotData: FlDotData(
                  show: true,
                  checkToShowDot: (spot, bar) => spot.x == points.last.dayOffset,
                  getDotPainter: (s, pc, b, i) => FlDotCirclePainter(
                    radius: 4,
                    color: p.series1,
                    strokeWidth: 2,
                    strokeColor: p.surface,
                  ),
                ),
                belowBarData: BarAreaData(
                  show: true,
                  color: p.series1.withValues(alpha: 0.10),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
