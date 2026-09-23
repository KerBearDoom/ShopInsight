import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';

/// 多折线图里的一条线。
class LineSeries {
  final String name;
  final List<double> values;
  final int colorIndex;

  const LineSeries({required this.name, required this.values, required this.colorIndex});
}

/// 多折线图 —— 几条**同量纲**的序列随时间变化。
///
/// ⚠️ 刻意不做双轴：可视化规范里「双 y 轴」是排第一的图表错误，
/// 两条不同量纲的线放一起，交点位置完全是刻度造的假象。
/// 这里几条线都是百分比（加购率/购买率），同轴才有可比性；
/// 绝对量另有分组柱状图那张，两者分开看。
///
/// 样式沿用 `trend_chart.dart`：2px 线、发丝网格、末端点带 2px 表面色环。
class MultiLineChart extends StatelessWidget {
  final List<String> xLabels;
  final List<LineSeries> series;

  /// y 轴数值的显示方式（是百分比还是绝对值）。
  final bool asPercent;

  const MultiLineChart({
    super.key,
    required this.xLabels,
    required this.series,
    this.asPercent = true,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    if (xLabels.isEmpty || series.isEmpty) {
      return SizedBox(
        height: 240,
        child: Center(
          child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13)),
        ),
      );
    }

    final maxV =
        series.expand((s) => s.values).fold<double>(0, (a, b) => a > b ? a : b);
    final niceMax = (maxV * 1.2).clamp(1.0, double.infinity);

    String fmt(double v) =>
        asPercent ? '${v.toStringAsFixed(1)}%' : v.toStringAsFixed(0);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        SizedBox(
          height: 240,
          child: LineChart(
            LineChartData(
              minX: 0,
              maxX: (xLabels.length - 1).toDouble(),
              minY: 0,
              maxY: niceMax,
              gridData: FlGridData(
                show: true,
                drawVerticalLine: false,
                horizontalInterval: niceMax / 4,
                getDrawingHorizontalLine: (_) => FlLine(color: p.grid, strokeWidth: 1),
              ),
              borderData: FlBorderData(show: false),
              titlesData: FlTitlesData(
                topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
                rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
                leftTitles: AxisTitles(
                  sideTitles: SideTitles(
                    showTitles: true,
                    reservedSize: 46,
                    interval: niceMax / 4,
                    getTitlesWidget: (value, meta) => Padding(
                      padding: const EdgeInsets.only(right: 6),
                      child: Text(fmt(value), style: TextStyle(fontSize: 11, color: p.muted)),
                    ),
                  ),
                ),
                bottomTitles: AxisTitles(
                  sideTitles: SideTitles(
                    showTitles: true,
                    reservedSize: 26,
                    interval: (xLabels.length / 2).clamp(1, 1 << 30).toDouble(),
                    getTitlesWidget: (value, meta) {
                      final i = value.round();
                      if (i < 0 || i >= xLabels.length) return const SizedBox.shrink();
                      final c = xLabels[i];
                      return Padding(
                        padding: const EdgeInsets.only(top: 6),
                        child: Text(c.length >= 10 ? c.substring(5) : c,
                            style: TextStyle(fontSize: 11, color: p.muted)),
                      );
                    },
                  ),
                ),
              ),
              lineTouchData: LineTouchData(
                touchTooltipData: LineTouchTooltipData(
                  getTooltipColor: (_) => p.surface,
                  getTooltipItems: (touched) => touched.map((spot) {
                    final s = series[spot.barIndex];
                    return LineTooltipItem(
                      '${xLabels[spot.x.round().clamp(0, xLabels.length - 1)]}\n',
                      TextStyle(fontSize: 11, color: p.ink2),
                      children: [
                        TextSpan(
                          text: '${s.name}  ${fmt(spot.y)}',
                          style: TextStyle(
                            fontSize: 12,
                            color: p.ink,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ],
                    );
                  }).toList(),
                ),
              ),
              lineBarsData: [
                for (final s in series)
                  LineChartBarData(
                    spots: [
                      for (var i = 0; i < s.values.length; i++)
                        FlSpot(i.toDouble(), s.values[i]),
                    ],
                    isCurved: false,
                    color: p.seriesAt(s.colorIndex),
                    barWidth: 2,
                    isStrokeCapRound: true,
                    isStrokeJoinRound: true,
                    // 多线图不画点 —— 线多了点会糊成一片。只在末端标一个
                    dotData: FlDotData(
                      show: true,
                      checkToShowDot: (spot, bar) => spot.x == (s.values.length - 1),
                      getDotPainter: (spot, percent, bar, index) => FlDotCirclePainter(
                        radius: 4,
                        color: p.seriesAt(s.colorIndex),
                        strokeWidth: 2,
                        strokeColor: p.surface,
                      ),
                    ),
                    // 不给面积填充：两条线都有填充时，上层的会盖住下层
                    belowBarData: BarAreaData(show: false),
                  ),
              ],
            ),
          ),
        ),
        const SizedBox(height: 12),
        // 多序列必须有图例
        Wrap(
          spacing: 16,
          children: [
            for (final s in series)
              Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Container(
                    width: 14,
                    height: 3,
                    decoration: BoxDecoration(
                      color: p.seriesAt(s.colorIndex),
                      borderRadius: BorderRadius.circular(2),
                    ),
                  ),
                  const SizedBox(width: 6),
                  Text(s.name, style: TextStyle(fontSize: 12, color: p.ink2)),
                ],
              ),
          ],
        ),
      ],
    );
  }
}
