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
  /// 首次加载时显示的补充说明。取数慢的页面（比如商品排行要全表扫
  /// 1.1 亿行）用它告诉用户"大概要等多久"，免得以为页面卡死了。
  final String? loadingHint;
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
    this.loadingHint,
  });

  @override
  State<RankingPage> createState() => _RankingPageState();
}

class _RankingPageState extends State<RankingPage> {
  static const _refreshInterval = Duration(seconds: 10);

  List<RankingItem> _items = const [];
  String? _error;
  Timer? _timer;
  bool _loading = false;

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
    // 在途保护：定时器是 10 秒一发，而商品排行那个接口要跑 20 多秒
    // （全表扫 1.1 亿行）。不挡住的话上一发还没回来就又发一发，
    // 几个全表扫描在 ClickHouse 上叠着跑，越跑越慢。
    if (_loading) return;
    setState(() => _loading = true);

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
    } finally {
      if (mounted) setState(() => _loading = false);
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
          // 只在"首次加载"（还没有任何数据）时占位。已有数据后的刷新
          // 不显示，免得每 10 秒闪一下转圈。
          else if (_items.isEmpty && _loading)
            _LoadingPlaceholder(hint: widget.loadingHint)
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

/// 首次加载占位。
///
/// 没有它的时候，取数慢的页面会先显示一个**空表格** —— 用户分不清
/// 是"还没加载完"还是"这个页面就是空的"，看起来像坏了。
class _LoadingPlaceholder extends StatelessWidget {
  final String? hint;

  const _LoadingPlaceholder({this.hint});

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    return Card(
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 64),
        child: Column(
          children: [
            const SizedBox(
              width: 22,
              height: 22,
              child: CircularProgressIndicator(strokeWidth: 2),
            ),
            const SizedBox(height: 16),
            Text('正在加载…', style: TextStyle(fontSize: 13, color: p.ink2)),
            if (hint != null) ...[
              const SizedBox(height: 6),
              Text(hint!,
                  textAlign: TextAlign.center,
                  style: TextStyle(fontSize: 11.5, color: p.ink2)),
            ],
          ],
        ),
      ),
    );
  }
}
