import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../utils/format.dart';

/// PV 趋势折线图。
///
/// 样式对齐可视化规范：
///   · 线宽 2px、圆角连接
///   · 面积填充用序列色 10% 透明度（淡色 wash，不是实心块）
///   · 网格是发丝线、实线（不用虚线）、颜色退后
///   · **单序列不加图例** —— 只有一个颜色，卡片标题已经说明画的是什么
///   · 坐标轴文字用弱化色，绝不使用序列色
class TrendChart extends StatelessWidget {
  final List<WindowMetric> data;

  const TrendChart({super.key, required this.data});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    if (data.isEmpty) {
      return SizedBox(
        height: 220,
        child: Center(
          child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13)),
        ),
      );
    }

    // 接口按时间倒序返回，画图要正序
    final rows = data.reversed.toList();

    final maxPv = rows.map((e) => e.pv).reduce((a, b) => a > b ? a : b);
    // y 轴上界取整到 200 的倍数，让刻度是干净的数字
    final niceMax = ((maxPv / 200).ceil() * 200).clamp(200, 1 << 30).toDouble();

    final spots = <FlSpot>[
      for (var i = 0; i < rows.length; i++) FlSpot(i.toDouble(), rows[i].pv.toDouble()),
    ];

    return SizedBox(
      height: 240,
      child: LineChart(
        LineChartData(
          minX: 0,
          maxX: (rows.length - 1).toDouble(),
          minY: 0,
          maxY: niceMax,
          // 只留水平网格；竖直网格对「随时间变化」的读图没有帮助
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
                  child: Text(
                    compact(value),
                    style: TextStyle(fontSize: 11, color: p.muted),
                    textAlign: TextAlign.right,
                  ),
                ),
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 26,
                // 只标首/中/尾三个，避免刻度文字挤在一起
                interval: (rows.length / 2).clamp(1, 1 << 30).toDouble(),
                getTitlesWidget: (value, meta) {
                  final i = value.round();
                  if (i < 0 || i >= rows.length) return const SizedBox.shrink();
                  final t = rows[i].windowStart.length >= 16
                      ? rows[i].windowStart.substring(11, 16) // HH:MM
                      : rows[i].windowStart;
                  return Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text(t, style: TextStyle(fontSize: 11, color: p.muted)),
                  );
                },
              ),
            ),
          ),
          lineTouchData: LineTouchData(
            touchTooltipData: LineTouchTooltipData(
              getTooltipColor: (_) => p.surface,
              getTooltipItems: (touched) => touched.map((spot) {
                final i = spot.x.round().clamp(0, rows.length - 1);
                final r = rows[i];
                return LineTooltipItem(
                  '${r.windowStart}\n',
                  TextStyle(fontSize: 11, color: p.ink2),
                  children: [
                    TextSpan(
                      text: 'PV  ${full(r.pv)}\n',
                      style: TextStyle(fontSize: 12, color: p.ink, fontWeight: FontWeight.w600),
                    ),
                    TextSpan(
                      text: 'UV  ${full(r.uv)}',
                      style: TextStyle(fontSize: 12, color: p.ink2),
                    ),
                  ],
                );
              }).toList(),
            ),
          ),
          lineBarsData: [
            LineChartBarData(
              spots: spots,
              isCurved: false,
              color: p.series1,
              barWidth: 2,
              isStrokeCapRound: true,
              isStrokeJoinRound: true,
              // 中间的点不画，只在末端标一个 —— 每个点都标会变成噪声
              dotData: FlDotData(
                show: true,
                checkToShowDot: (spot, barData) => spot.x == spots.last.x,
                getDotPainter: (spot, percent, barData, index) => FlDotCirclePainter(
                  radius: 4, // 直径 8px
                  color: p.series1,
                  // 2px 表面色环：压在线上或与其它标记重叠时仍然看得清
                  strokeWidth: 2,
                  strokeColor: p.surface,
                ),
              ),
              belowBarData: BarAreaData(
                show: true,
                color: p.series1.withValues(alpha: 0.10), // 10% 淡色面积
              ),
            ),
          ],
        ),
      ),
    );
  }
}
