import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';
import '../../utils/format.dart';

/// 横向条形图的一条。
class HBarItem {
  final String label;

  /// 决定条形长度和排序。
  final double value;

  /// 数值的显示文本（条形右侧）。不传就用 [value] 格式化。
  final String? valueText;

  /// 悬停提示里的补充行，例如「购买 123 · 转化率 4.2%」。
  final String? detail;

  const HBarItem({
    required this.label,
    required this.value,
    this.valueText,
    this.detail,
  });
}

/// 横向条形图 —— 排行/量级对比的形态。
///
/// 为什么不用 fl_chart 的 BarChart：它的柱子只有竖直方向，横向得靠旋转
/// （标签会跟着转）。而排行场景的标签是类目码、品牌名这类长文本，
/// 横向摆才放得下。所以这里用普通 Widget 排 —— 项目里原有的漏斗条
/// （`insight_page.dart` 的 `_FunnelBar`）就是这么做的，沿用同一套。
///
/// 单序列，所以不需要图例，标题已经说明了这是什么。
class HBarChart extends StatelessWidget {
  final List<HBarItem> items;

  /// 条形的槽位色，默认取 series[0]。
  final int colorIndex;

  final double barHeight;

  const HBarChart({
    super.key,
    required this.items,
    this.colorIndex = 0,
    this.barHeight = 22,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    if (items.isEmpty) {
      return Text('暂无数据', style: TextStyle(fontSize: 13, color: p.muted));
    }

    // 条长按最大值归一化 —— 排行榜里绝对量没有意义，相对差距才有
    final maxV = items.map((e) => e.value).reduce((a, b) => a > b ? a : b);
    final labelW = items.map((e) => e.label.length).reduce((a, b) => a > b ? a : b);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        for (final it in items)
          Padding(
            padding: const EdgeInsets.symmetric(vertical: 3),
            child: _Bar(
              item: it,
              ratio: maxV <= 0 ? 0 : it.value / maxV,
              color: p.seriesAt(colorIndex),
              labelWidth: (labelW * 8.0 + 8).clamp(56.0, 150.0),
              barHeight: barHeight,
            ),
          ),
      ],
    );
  }
}

class _Bar extends StatelessWidget {
  final HBarItem item;
  final double ratio;
  final Color color;
  final double labelWidth;
  final double barHeight;

  const _Bar({
    required this.item,
    required this.ratio,
    required this.color,
    required this.labelWidth,
    required this.barHeight,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    final tip = item.detail == null ? item.label : '${item.label}\n${item.detail}';

    return Tooltip(
      message: tip,
      waitDuration: const Duration(milliseconds: 150),
      child: Row(
        children: [
          SizedBox(
            width: labelWidth,
            child: Text(
              item.label,
              overflow: TextOverflow.ellipsis,
              style: TextStyle(fontSize: 12.5, color: p.ink),
            ),
          ),
          Expanded(
            child: LayoutBuilder(builder: (context, c) {
              // 条形宽度按真实比例，但留一个最小可见宽度 ——
              // 长尾里 1% 的项在真实比例下几乎看不见，那就失去了
              // 「一眼看出量级差」的意义。精确值由右侧数字表达。
              final w = (c.maxWidth * ratio).clamp(3.0, c.maxWidth);
              return Align(
                alignment: Alignment.centerLeft,
                child: Container(
                  width: w,
                  height: barHeight,
                  decoration: BoxDecoration(
                    // 数据端 4px 圆角、贴基线端方角（规范里的标记形状）
                    color: color.withValues(alpha: 0.9),
                    borderRadius: const BorderRadius.only(
                      topRight: Radius.circular(4),
                      bottomRight: Radius.circular(4),
                    ),
                  ),
                ),
              );
            }),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 84,
            child: Text(
              item.valueText ?? compact(item.value),
              textAlign: TextAlign.right,
              // 文字用文字色，不用序列色 —— 颜色只负责身份，数值由数字表达
              style: TextStyle(
                fontSize: 12.5,
                color: p.ink,
                fontFeatures: const [FontFeature.tabularFigures()],
              ),
            ),
          ),
        ],
      ),
    );
  }
}
