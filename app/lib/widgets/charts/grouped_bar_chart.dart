import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';
import '../../utils/format.dart';

/// 分组柱状图里的一条序列。
class BarSeries {
  final String name;
  final List<double> values;
  final int colorIndex;

  const BarSeries({required this.name, required this.values, required this.colorIndex});
}

/// 分组柱状图 —— 同一时间点上几个量并排对比。
///
/// 排行页的数据是「一个维度按另一个维度排序」，这里是「同一天三个环节」，
/// 所以用分组柱而不是条形：读完一组就知道那天的漏斗长什么样。
///
/// **多序列必须有图例**（规范要求）—— 身份识别不能只靠颜色。
/// 浅色模式下有两个槽位色的对比度低于 3:1，图例和悬停提示是那两处的兜底。
class GroupedBarChart extends StatelessWidget {
  final List<String> categories;
  final List<BarSeries> series;
  final String unit;

  const GroupedBarChart({
    super.key,
    required this.categories,
    required this.series,
    this.unit = '',
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    if (categories.isEmpty || series.isEmpty) {
      return SizedBox(
        height: 240,
        child: Center(
          child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13)),
        ),
      );
    }

    final maxV = series
        .expand((s) => s.values)
        .fold<double>(0, (a, b) => a > b ? a : b);
    final niceMax = (maxV * 1.1).clamp(1.0, double.infinity);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        SizedBox(
          height: 240,
          child: BarChart(
            BarChartData(
              maxY: niceMax,
              // 组间距给足，组内柱贴紧 —— 这样「一组」在视觉上是一个整体
              alignment: BarChartAlignment.spaceAround,
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
                      child: Text(compact(value), style: TextStyle(fontSize: 11, color: p.muted)),
                    ),
                  ),
                ),
                bottomTitles: AxisTitles(
                  sideTitles: SideTitles(
                    showTitles: true,
                    reservedSize: 26,
                    getTitlesWidget: (value, meta) {
                      final i = value.round();
                      if (i < 0 || i >= categories.length) return const SizedBox.shrink();
                      // 日期只显示后半段，避免挤在一起
                      final c = categories[i];
                      final t = c.length >= 10 ? c.substring(5) : c;
                      return Padding(
                        padding: const EdgeInsets.only(top: 6),
                        child: Text(t, style: TextStyle(fontSize: 11, color: p.muted)),
                      );
                    },
                  ),
                ),
              ),
              barTouchData: BarTouchData(
                touchTooltipData: BarTouchTooltipData(
                  getTooltipColor: (_) => p.surface,
                  getTooltipItem: (group, groupIndex, rod, rodIndex) {
                    final s = series[rodIndex];
                    final c = categories[group.x];
                    return BarTooltipItem(
                      '$c\n',
                      TextStyle(fontSize: 11, color: p.ink2),
                      children: [
                        TextSpan(
                          text: '${s.name}  ',
                          style: TextStyle(fontSize: 12, color: p.ink2),
                        ),
                        TextSpan(
                          text: full(rod.toY),
                          style: TextStyle(
                            fontSize: 12,
                            color: p.ink,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ],
                    );
                  },
                ),
              ),
              barGroups: [
                for (var i = 0; i < categories.length; i++)
                  BarChartGroupData(
                    x: i,
                    barsSpace: 2,
                    barRods: [
                      for (final s in series)
                        BarChartRodData(
                          toY: i < s.values.length ? s.values[i] : 0,
                          color: p.seriesAt(s.colorIndex).withValues(alpha: 0.9),
                          width: 7,
                          // 数据端 4px 圆角、基线端方角
                          borderRadius: const BorderRadius.only(
                            topLeft: Radius.circular(4),
                            topRight: Radius.circular(4),
                          ),
                        ),
                    ],
                  ),
              ],
            ),
          ),
        ),
        const SizedBox(height: 12),
        Wrap(
          spacing: 16,
          children: [
            for (final s in series)
              Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  Container(
                    width: 9,
                    height: 9,
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
