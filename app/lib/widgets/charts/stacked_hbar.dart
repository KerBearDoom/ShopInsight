import 'package:flutter/material.dart';

import '../../theme/app_theme.dart';

/// 堆叠条形图里的一段。
class StackSegment {
  final String label;
  final double value;

  /// 槽位色索引（跟实体绑定，不跟排名走）。
  final int colorIndex;

  /// 悬停提示里的补充信息。
  final String? detail;

  const StackSegment({
    required this.label,
    required this.value,
    required this.colorIndex,
    this.detail,
  });
}

/// 横向堆叠条形图 —— 表达**部分与整体**的关系。
///
/// **为什么不用饼图/环形图**：可视化规范的形态表里，「部分-整体」对应的是
/// 堆叠条形图。这不是偏好问题 —— 选型时试过环形图，跑配色验证器时硬失败：
/// 四个分类色在"任意两色都可能被拿来比较"的场景下，橙 ↔ 黄的正常视力
/// ΔE 只有 13.7 和 10.6，都低于 15 的硬门槛，穷举了 5 组四色组合没有一组能过。
///
/// 堆叠条形只需要保证**相邻两段**能分辨，同一组颜色就通过了。
///
/// 颜色只负责身份，占比由条段长度 + 下方图例的数字共同表达 ——
/// 这样即使某段的对比度偏低（浅色模式下有两个色低于 3:1），
/// 读数也不依赖颜色。
class StackedHBar extends StatelessWidget {
  final List<StackSegment> segments;

  /// 条形上方的一句话说明，例如「用户数占比」。
  final String? caption;

  const StackedHBar({super.key, required this.segments, this.caption});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    final total = segments.fold<double>(0, (s, e) => s + e.value);

    if (segments.isEmpty || total <= 0) {
      return Text('暂无数据', style: TextStyle(fontSize: 13, color: p.muted));
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        if (caption != null) ...[
          Text(caption!, style: TextStyle(fontSize: 12.5, color: p.ink2)),
          const SizedBox(height: 8),
        ],

        // ── 条形本体 ──────────────────────────────────────────────────
        ClipRRect(
          borderRadius: BorderRadius.circular(6),
          child: SizedBox(
            height: 30,
            child: Row(
              children: [
                for (var i = 0; i < segments.length; i++)
                  Expanded(
                    // 用整数权重避免浮点累积误差导致最后一段溢出
                    flex: ((segments[i].value / total) * 10000).round().clamp(1, 10000),
                    child: _Segment(
                      seg: segments[i],
                      pct: segments[i].value / total * 100,
                      color: p.seriesAt(segments[i].colorIndex),
                      // 段与段之间留 2px 的表面色缝隙（规范要求），
                      // 这样相邻两段即使色相接近也有一条清晰的分界
                      isLast: i == segments.length - 1,
                    ),
                  ),
              ],
            ),
          ),
        ),

        const SizedBox(height: 12),

        // ── 图例 ─────────────────────────────────────────────────────
        // 双序列以上必须有图例 —— 身份识别不能只靠颜色
        for (final s in segments)
          Padding(
            padding: const EdgeInsets.symmetric(vertical: 2),
            child: Row(
              children: [
                _Dot(color: p.seriesAt(s.colorIndex)),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    s.label,
                    style: TextStyle(fontSize: 12.5, color: p.ink),
                  ),
                ),
                Text(
                  '${(s.value / total * 100).toStringAsFixed(1)}%',
                  style: TextStyle(
                    fontSize: 12.5,
                    color: p.ink2,
                    fontFeatures: const [FontFeature.tabularFigures()],
                  ),
                ),
              ],
            ),
          ),
      ],
    );
  }
}

class _Segment extends StatelessWidget {
  final StackSegment seg;
  final double pct;
  final Color color;
  final bool isLast;

  const _Segment({
    required this.seg,
    required this.pct,
    required this.color,
    required this.isLast,
  });

  @override
  Widget build(BuildContext context) {
    return Tooltip(
      message: seg.detail == null
          ? '${seg.label}  ${pct.toStringAsFixed(1)}%'
          : '${seg.label}  ${pct.toStringAsFixed(1)}%\n${seg.detail}',
      waitDuration: const Duration(milliseconds: 150),
      child: Container(
        color: color.withValues(alpha: 0.9),
        // 右侧用表面色画一条 2px 的缝，隔开相邻段
        margin: EdgeInsets.only(right: isLast ? 0 : 2),
      ),
    );
  }
}

class _Dot extends StatelessWidget {
  final Color color;
  const _Dot({required this.color});

  @override
  Widget build(BuildContext context) => Container(
        width: 9,
        height: 9,
        decoration: BoxDecoration(
          color: color,
          borderRadius: BorderRadius.circular(2),
        ),
      );
}
