import 'dart:async';

import 'package:flutter/material.dart';

import '../api/analytics_api.dart';
import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../utils/format.dart';
import '../widgets/stat_tile.dart';
import '../widgets/trend_chart.dart';

/// 实时总览页：KPI 三卡 + PV 趋势折线图。
///
/// 每 5 秒自动拉一次数据。这是**轮询**而不是服务端推送 —— 对这个规模的
/// 数据量够用，实现也简单；如果以后要更实时，可以换成 SSE。
class OverviewPage extends StatefulWidget {
  const OverviewPage({super.key});

  @override
  State<OverviewPage> createState() => _OverviewPageState();
}

class _OverviewPageState extends State<OverviewPage> {
  static const _refreshInterval = Duration(seconds: 5);

  List<WindowMetric> _data = const [];
  String? _error;
  DateTime? _updatedAt;
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
      final rows = await AnalyticsApi.overview(limit: 60);
      if (!mounted) return;
      setState(() {
        _data = rows;
        _error = null;
        _updatedAt = DateTime.now();
      });
    } catch (e) {
      if (!mounted) return;
      setState(() => _error = '$e');
    }
  }

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    final latest = _data.isNotEmpty ? _data.first : null;

    return RefreshIndicator(
      onRefresh: _load,
      child: ListView(
        padding: const EdgeInsets.all(20),
        children: [
          _Header(updatedAt: _updatedAt, error: _error),
          const SizedBox(height: 18),

          // KPI 三卡：窄屏竖排，宽屏横排
          LayoutBuilder(builder: (context, c) {
            final tiles = [
              StatTile(
                label: '窗口内 PV（浏览量）',
                value: latest == null ? '—' : full(latest.pv),
                meta: latest?.windowStart,
              ),
              StatTile(
                label: '窗口内 UV（独立访客）',
                value: latest == null ? '—' : full(latest.uv),
                meta: latest?.windowStart,
              ),
              StatTile(
                label: '窗口内成交额',
                value: latest == null ? '—' : money(latest.gmv),
                meta: latest?.windowStart,
              ),
            ];
            if (c.maxWidth < 720) {
              return Column(
                children: [
                  for (final t in tiles) ...[t, const SizedBox(height: 12)],
                ],
              );
            }
            return Row(
              children: [
                for (var i = 0; i < tiles.length; i++) ...[
                  Expanded(child: tiles[i]),
                  if (i < tiles.length - 1) const SizedBox(width: 12),
                ],
              ],
            );
          }),

          const SizedBox(height: 18),

          Card(
            child: Padding(
              padding: const EdgeInsets.fromLTRB(20, 18, 20, 20),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('PV 趋势',
                      style: TextStyle(fontSize: 14, fontWeight: FontWeight.w600, color: p.ink)),
                  const SizedBox(height: 2),
                  Text('按 1 分钟事件时间窗口聚合，最近 ${_data.length} 个窗口',
                      style: TextStyle(fontSize: 12.5, color: p.ink2)),
                  const SizedBox(height: 18),
                  TrendChart(data: _data),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

/// 页头：标题 + 最后更新时间 + 数据来源说明。
class _Header extends StatelessWidget {
  final DateTime? updatedAt;
  final String? error;

  const _Header({this.updatedAt, this.error});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text('实时总览',
            style: TextStyle(fontSize: 20, fontWeight: FontWeight.w600, color: p.ink)),
        const SizedBox(height: 4),
        Row(children: [
          Container(
            width: 7,
            height: 7,
            decoration: BoxDecoration(
              color: error == null ? const Color(0xFF0CA30C) : const Color(0xFFD03B3B),
              shape: BoxShape.circle,
            ),
          ),
          const SizedBox(width: 6),
          Expanded(
            child: Text(
              error != null
                  ? '取数失败：$error'
                  : updatedAt == null
                      ? '加载中…'
                      : '已更新 ${updatedAt!.hour.toString().padLeft(2, '0')}:'
                          '${updatedAt!.minute.toString().padLeft(2, '0')}:'
                          '${updatedAt!.second.toString().padLeft(2, '0')}',
              style: TextStyle(fontSize: 12, color: error != null ? const Color(0xFFD03B3B) : p.muted),
              overflow: TextOverflow.ellipsis,
            ),
          ),
        ]),
      ],
    );
  }
}
