import 'package:flutter/material.dart';

import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../utils/format.dart';

/// 排行表。品牌 / 类目 / 商品三个排行页共用。
///
/// 用表格而不是条形图：这些排行的主体是**高基数标识**（品牌名、类目 ID、
/// 商品 ID），条形图适合表达量级对比，而这里用户更需要「看到具体数字」。
class RankingTable extends StatelessWidget {
  final List<RankingItem> items;
  final String nameHeader;
  /// 是否显示 UV 列。只有商品排行接口返回 uv，其余接口不返回
  /// （跨窗口的 UV 不可加，后端刻意不提供）。
  final bool showUv;

  const RankingTable({
    super.key,
    required this.items,
    required this.nameHeader,
    this.showUv = false,
  });

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    if (items.isEmpty) {
      return Padding(
        padding: const EdgeInsets.symmetric(vertical: 28),
        child: Center(child: Text('暂无数据', style: TextStyle(color: p.muted, fontSize: 13))),
      );
    }

    final rows = <DataRow>[
      for (var i = 0; i < items.length; i++)
        DataRow(cells: [
          DataCell(Row(children: [
            SizedBox(
              width: 22,
              child: Text('${i + 1}', style: TextStyle(color: p.muted, fontSize: 12.5)),
            ),
            Expanded(
              child: Text(
                items[i].name,
                overflow: TextOverflow.ellipsis,
                style: TextStyle(color: p.ink, fontSize: 13),
              ),
            ),
          ])),
          _numCell(compact(items[i].pv), p.ink2),
          if (showUv) _numCell(compact(items[i].uv), p.ink2),
          _numCell(full(items[i].purchaseCnt), p.ink2),
          _numCell(money(items[i].gmv), p.ink),
          _numCell(
            items[i].purchaseCnt == 0 ? '—' : money2(items[i].avgOrderValue),
            p.ink2,
          ),
        ]),
    ];

    return SingleChildScrollView(
      scrollDirection: Axis.horizontal,
      child: DataTable(
        headingRowHeight: 34,
        dataRowMinHeight: 36,
        dataRowMaxHeight: 44,
        columnSpacing: 20,
        horizontalMargin: 0,
        headingTextStyle: TextStyle(fontSize: 12, color: p.muted, fontWeight: FontWeight.w500),
        dividerThickness: 0.6,
        columns: [
          DataColumn(label: Text(nameHeader), numeric: false),
          const DataColumn(label: Text('PV'), numeric: true),
          if (showUv) const DataColumn(label: Text('UV'), numeric: true),
          const DataColumn(label: Text('购买数'), numeric: true),
          const DataColumn(label: Text('成交额'), numeric: true),
          const DataColumn(label: Text('客单价'), numeric: true),
        ],
        rows: rows,
      ),
    );
  }

  /// 数字列统一右对齐 + 等宽数字（表格列需要纵向对齐，这正是 tabular-nums 的场景）。
  DataCell _numCell(String text, Color color) => DataCell(
        Text(
          text,
          style: TextStyle(
            fontSize: 13,
            color: color,
            fontFeatures: const [FontFeature.tabularFigures()],
          ),
        ),
      );
}
