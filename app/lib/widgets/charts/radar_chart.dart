import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';

/// 雷达图上的一个实体（一个用户分群）。
class RadarEntity {
  final String name;

  /// 各维度的原始值，顺序与 [axes] 对应。
  final List<double> values;

  final int colorIndex;

  const RadarEntity({
    required this.name,
    required this.values,
    required this.colorIndex,
  });
}

/// 雷达图 —— 三个维度对比多个实体。
///
/// 这是雷达图**唯一站得住的用法**：轴是同一组指标（R/F/M），
/// 不同实体（用户分群）的"形状"直接可比。换成柱状图，三个维度 × 三个分群
/// 要画九根柱子，反而看不出形状差异。
///
/// ⚠️ **各维度按最大值归一化**，画出来是「相对最强者的比例」不是绝对值。
/// 轴刻度隐藏，数值走悬停 —— 雷达图的读法是看形状，不是看小数点。
///
/// ⚠️ 只放 **3 个分群**：四色在"任意两色都可能被比较"的场景下过不了
/// 配色验证器的硬门槛（实测橙↔黄正常视力 ΔE 只有 13.7）。
///
/// 实现上是个 StatefulWidget：fl_chart 1.2.0 的 RadarChart **没有内建 tooltip**
/// （不像折线/柱状那样有 touchTooltipData），只给了 touchCallback，
/// 所以悬停反馈要自己接 —— 鼠标移到某个分群上，下方面板显示它各维度的真实值。
class RadarCompareChart extends StatefulWidget {
  final List<String> axes;
  final List<RadarEntity> entities;

  /// 哪些维度是「越小越好」，画图时反向 ——
  /// 否则「最近一次消费距今 5 天」（说明活跃）会画成一个很小的角，和直觉相反。
  final bool invertRecency;

  const RadarCompareChart({
    super.key,
    required this.axes,
    required this.entities,
    this.invertRecency = true,
  });

  @override
  State<RadarCompareChart> createState() => _RadarCompareChartState();
}

class _RadarCompareChartState extends State<RadarCompareChart> {
  int? _hover;

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    final axes = widget.axes;
    final entities = widget.entities;

    if (axes.length < 3 || entities.isEmpty) {
      return SizedBox(
        height: 240,
        child: Center(
          child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13)),
        ),
      );
    }

    // 逐维度归一化：每个轴按该轴的最大值缩放
    final norm = <List<double>>[];
    for (var a = 0; a < axes.length; a++) {
      final col = [for (final e in entities) e.values[a]];
      final maxV = col.fold<double>(0, (x, y) => x > y ? x : y);
      norm.add([
        for (final v in col)
          maxV <= 0
              ? 0
              : ((a == 0 && widget.invertRecency) ? (maxV - v) / maxV : v / maxV),
      ]);
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        SizedBox(
          height: 250,
          child: RadarChart(
            RadarChartData(
              radarShape: RadarShape.polygon,
              tickCount: 4,
              // 刻度值不显示 —— 归一化后的数字没有业务含义，显示反而误导
              ticksTextStyle:
                  const TextStyle(color: Colors.transparent, fontSize: 0),
              titleTextStyle: TextStyle(fontSize: 12, color: p.ink2),
              titlePositionPercentageOffset: 0.14,
              getTitle: (index, angle) =>
                  RadarChartTitle(text: axes[index % axes.length]),
              gridBorderData: BorderSide(color: p.grid, width: 1),
              tickBorderData: BorderSide(color: p.grid, width: 1),
              radarBorderData: BorderSide(color: p.grid, width: 1),
              radarBackgroundColor: Colors.transparent,
              radarTouchData: RadarTouchData(
                enabled: true,
                touchCallback: (event, response) {
                  final spot = response?.touchedSpot;
                  final i = spot?.touchedDataSetIndex;
                  if (_hover != i) setState(() => _hover = i);
                },
              ),
              dataSets: [
                for (var i = 0; i < entities.length; i++)
                  RadarDataSet(
                    // 每个实体都要给全部维度的值，少一个 assert 就会挂
                    dataEntries: [
                      for (var a = 0; a < axes.length; a++)
                        RadarEntry(value: norm[a][i].clamp(0.0, 1.0)),
                    ],
                    // 半透明填充：多个实体叠在一起时，重叠区颜色会加深
                    fillColor:
                        p.seriesAt(entities[i].colorIndex).withValues(alpha: 0.18),
                    borderColor: p.seriesAt(entities[i].colorIndex),
                    borderWidth: 2,
                    entryRadius: 3,
                  ),
              ],
            ),
          ),
        ),
        const SizedBox(height: 10),

        // ── 悬停反馈面板 ────────────────────────────────────────────────
        // 没悬停时显示图例；悬停时换成那个分群的真实值
        if (_hover == null)
          Wrap(
            spacing: 16,
            children: [
              for (final e in entities)
                Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    _dot(p.seriesAt(e.colorIndex)),
                    const SizedBox(width: 6),
                    Text(e.name, style: TextStyle(fontSize: 12, color: p.ink2)),
                  ],
                ),
            ],
          )
        else
          Container(
            width: double.infinity,
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
            decoration: BoxDecoration(
              color: p.surface,
              border: Border.all(color: p.grid),
              borderRadius: BorderRadius.circular(6),
            ),
            child: Row(
              children: [
                _dot(p.seriesAt(entities[_hover!].colorIndex)),
                const SizedBox(width: 8),
                Text(entities[_hover!].name,
                    style: TextStyle(
                        fontSize: 12.5, color: p.ink, fontWeight: FontWeight.w600)),
                const SizedBox(width: 14),
                Expanded(
                  child: Text(
                    [
                      for (var a = 0; a < axes.length; a++)
                        '${axes[a]} ${entities[_hover!].values[a].toStringAsFixed(1)}',
                    ].join('   ·   '),
                    style: TextStyle(fontSize: 12, color: p.ink2),
                  ),
                ),
              ],
            ),
          ),

        const SizedBox(height: 6),
        Text(
          '各维度按最大值归一化，看的是形状差异而非绝对量'
          '${widget.invertRecency ? '；R（最近消费天数）已反向，越靠外越活跃' : ''}',
          style: TextStyle(fontSize: 11, color: p.muted),
        ),
      ],
    );
  }

  Widget _dot(Color c) => Container(
        width: 9,
        height: 9,
        decoration: BoxDecoration(color: c, borderRadius: BorderRadius.circular(2)),
      );
}
