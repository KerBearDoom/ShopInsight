import 'dart:async';

import 'package:flutter/material.dart';

import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../widgets/ranking_table.dart';

/// 通用的排行页。品牌 / 类目 / 商品三个页面结构完全一样，
/// 只有标题、列名和取数函数不同，所以抽成一个页面 + 传入参数。
class RankingPage extends StatefulWidget {
  final String title;
  final String subtitle;
  final String nameHeader;
  final bool showUv;
  /// 取数函数。传函数而不是直接把数据传进来，是为了让页面自己负责
  /// 加载状态和刷新。
  final Future<List<RankingItem>> Function(int limit) fetcher;

  const RankingPage({
    super.key,
    required this.title,
    required this.subtitle,
    required this.nameHeader,
    required this.fetcher,
    this.showUv = false,
  });

  @override
  State<RankingPage> createState() => _RankingPageState();
}

class _RankingPageState extends State<RankingPage> {
  static const _refreshInterval = Duration(seconds: 10);

  List<RankingItem> _items = const [];
  String? _error;
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
      final rows = await widget.fetcher(20);
      if (!mounted) return;
      setState(() {
        _items = rows;
        _error = null;
      });
    } catch (e) {
      if (!mounted) return;
      setState(() => _error = '$e');
    }
  }

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    return RefreshIndicator(
      onRefresh: _load,
      child: ListView(
        padding: const EdgeInsets.all(20),
        children: [
          Text(widget.title,
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.w600, color: p.ink)),
          const SizedBox(height: 4),
          Text(widget.subtitle, style: TextStyle(fontSize: 12.5, color: p.ink2)),
          const SizedBox(height: 18),
          if (_error != null)
            Padding(
              padding: const EdgeInsets.symmetric(vertical: 24),
              child: Text('取数失败：$_error',
                  style: const TextStyle(color: Color(0xFFD03B3B), fontSize: 13)),
            )
          else
            Card(
              child: Padding(
                padding: const EdgeInsets.fromLTRB(20, 12, 20, 12),
                child: RankingTable(
                  items: _items,
                  nameHeader: widget.nameHeader,
                  showUv: widget.showUv,
                ),
              ),
            ),
        ],
      ),
    );
  }
}
