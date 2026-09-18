import 'package:flutter/material.dart';

import '../theme/app_theme.dart';

/// KPI 卡片。
///
/// 按可视化规范的 stat tile 约定：label（句子式，不带冒号）+ value（半粗体）
/// + 可选的 meta（补充说明，用弱化色）。
///
/// 大数字刻意**不用** `tabular-nums` —— 等宽数字会让每个字都是 0 的宽度，
/// 大字号下像 121 这样的数会显得松散。等宽只用在需要纵向对齐的表格列里。
class StatTile extends StatelessWidget {
  final String label;
  final String value;
  final String? meta;

  const StatTile({super.key, required this.label, required this.value, this.meta});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    return Card(
      child: Padding(
        padding: const EdgeInsets.fromLTRB(18, 16, 18, 16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(label, style: TextStyle(fontSize: 12.5, color: p.ink2)),
            const SizedBox(height: 6),
            Text(
              value,
              style: TextStyle(
                fontSize: 30,
                fontWeight: FontWeight.w600,
                color: p.ink,
                letterSpacing: -0.5,
                height: 1.1,
              ),
            ),
            if (meta != null) ...[
              const SizedBox(height: 6),
              Text(meta!, style: TextStyle(fontSize: 12, color: p.muted)),
            ],
          ],
        ),
      ),
    );
  }
}
