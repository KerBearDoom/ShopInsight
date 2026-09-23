import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';
import '../../utils/format.dart';

/// 散点图的一个点。
class ScatterPoint {
  final String label;
  final double x;
  final double y;

  const ScatterPoint({required this.label, required this.x, required this.y});
}

/// 散点图 —— 用来看**二维分布**，找一维排行里看不见的异常。
///
/// 排行页用它画「浏览量 × 购买率」：一维排行只能告诉你谁卖得多，
/// 但看不出「谁被很多人看了却没买」—— 那才是详情页或定价有问题的信号。
///
/// 样式沿用 `trend_chart.dart` 定下的一套：发丝网格、轴文字用弱化色、
/// 点上带 2px 表面色环（重叠时仍分得清）。
class ScatterPlotChart extends StatelessWidget {
  final List<ScatterPoint> points;
  final String xUnit;
  final String yUnit;
  final int colorIndex;

  /// 点的半径。点多的时候可以调小。
  final double radius;

  const ScatterPlotChart({
    super.key,
    required this.points,
    required this.xUnit,
    required this.yUnit,
    this.colorIndex = 0,
    this.radius = 5,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    if (points.isEmpty) {
      return SizedBox(
        height: 240,
        child: Center(
          child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13)),
        ),
      );
    }

    final maxX = points.map((e) => e.x).reduce((a, b) => a > b ? a : b);
    final maxY = points.map((e) => e.y).reduce((a, b) => a > b ? a : b);

    // y 轴上界留 15% 余量，免得最高的点贴到顶边
    final niceMaxY = (maxY * 1.15).clamp(0.01, double.infinity);
    final niceMaxX = maxX * 1.05;

    final color = p.seriesAt(colorIndex);

    return SizedBox(
      height: 260,
      child: ScatterChart(
        ScatterChartData(
          minX: 0,
          maxX: niceMaxX,
          minY: 0,
          maxY: niceMaxY,
          // 两个方向都画网格：散点图是在二维里找位置，只画一个方向的网格
          // 会让「这个点偏左上还是偏右下」不好判断
          gridData: FlGridData(
            show: true,
            horizontalInterval: niceMaxY / 4,
            verticalInterval: niceMaxX / 4,
            getDrawingHorizontalLine: (_) => FlLine(color: p.grid, strokeWidth: 1),
            getDrawingVerticalLine: (_) => FlLine(color: p.grid, strokeWidth: 1),
          ),
          borderData: FlBorderData(show: false),
          titlesData: FlTitlesData(
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 46,
                interval: niceMaxY / 4,
                getTitlesWidget: (value, meta) => Padding(
                  padding: const EdgeInsets.only(right: 6),
                  child: Text('${(value * 100).toStringAsFixed(1)}%',
                      style: TextStyle(fontSize: 11, color: p.muted)),
                ),
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 26,
                interval: niceMaxX / 4,
                getTitlesWidget: (value, meta) => Padding(
                  padding: const EdgeInsets.only(top: 6),
                  child: Text(compact(value), style: TextStyle(fontSize: 11, color: p.muted)),
                ),
              ),
            ),
          ),
          scatterTouchData: ScatterTouchData(
            enabled: true,
            touchTooltipData: ScatterTouchTooltipData(
              getTooltipColor: (_) => p.surface,
              // 这个版本的回调只收一个 ScatterSpot（不像折线图那样给一组），
              // 所以按坐标反查回原始数据 —— 点数最多几十个，线性找足够
              getTooltipItems: (spot) {
                ScatterPoint? pt;
                for (final q in points) {
                  if (q.x == spot.x && q.y == spot.y) {
                    pt = q;
                    break;
                  }
                }
                if (pt == null) return null;
                return ScatterTooltipItem(
                  '${pt.label}\n',
                  textStyle: TextStyle(fontSize: 11, color: p.ink2),
                  children: [
                    TextSpan(
                      text: '$xUnit  ${compact(pt.x)}\n',
                      style: TextStyle(fontSize: 12, color: p.ink),
                    ),
                    TextSpan(
                      text: '$yUnit  ${(pt.y * 100).toStringAsFixed(2)}%',
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
          scatterSpots: [
            for (final pt in points)
              ScatterSpot(
                pt.x,
                pt.y,
                dotPainter: FlDotCirclePainter(
                  radius: radius,
                  color: color.withValues(alpha: 0.75),
                  // 2px 表面色环：点密集重叠时仍然分得清谁是谁
                  strokeWidth: 2,
                  strokeColor: p.surface,
                ),
              ),
          ],
        ),
      ),
    );
  }
}
