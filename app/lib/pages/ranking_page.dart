import 'dart:async';

import 'package:flutter/material.dart';

import '../models/metrics.dart';
import '../theme/app_theme.dart';
import '../utils/format.dart';
import '../widgets/charts/hbar_chart.dart';
import '../widgets/charts/scatter_chart.dart';
import '../widgets/charts/stacked_hbar.dart';
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
          else ...[
            // 三张图各回答一个不同的问题，自上而下：
            //   条形 → 谁在前面、差距多大
            //   散点 → 谁「被看了却没买」（一维排行看不出来的）
            //   堆叠 → 头部集中度如何
            Card(
              child: Padding(
                padding: const EdgeInsets.fromLTRB(20, 16, 20, 16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    _chartTitle(context, '成交额排行',
                        '条长按最大值归一化 —— 看差距，不看绝对量'),
                    const SizedBox(height: 14),
                    HBarChart(
                      items: [
                        for (final it in _items.take(10))
                          HBarItem(
                            label: it.name,
                            value: it.gmv,
                            valueText: money(it.gmv),
                            detail: '浏览 ${compact(it.pv)} · 购买 ${full(it.purchaseCnt)}',
                          ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 18),

            Card(
              child: Padding(
                padding: const EdgeInsets.fromLTRB(20, 16, 20, 16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    _chartTitle(context, '流量 × 转化率',
                        '横轴是曝光量，纵轴是购买率 —— 右下角那块是「看了不买」的'),
                    const SizedBox(height: 14),
                    ScatterPlotChart(
                      xUnit: '浏览量',
                      yUnit: '购买率',
                      points: [
                        for (final it in _items)
                          if (it.pv > 0)
                            ScatterPoint(
                              label: it.name,
                              x: it.pv.toDouble(),
                              y: it.purchaseCnt / it.pv,
                            ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 18),

            Card(
              child: Padding(
                padding: const EdgeInsets.fromLTRB(20, 16, 20, 16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    _chartTitle(context, '头部集中度',
                        '前三名拿走了多少成交额 —— 越高说明越依赖爆款'),
                    const SizedBox(height: 14),
                    StackedHBar(segments: _concentration()),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 18),

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
        ],
      ),
    );
  }

  /// 标题 + 一句话说明，说明写清楚「这张图该看什么」。
  Widget _chartTitle(BuildContext context, String title, String desc) {
    final p = AppPalette.of(context);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(title,
            style: TextStyle(fontSize: 14, fontWeight: FontWeight.w600, color: p.ink)),
        const SizedBox(height: 2),
        Text(desc, style: TextStyle(fontSize: 12, color: p.ink2)),
      ],
    );
  }

  /// 头部集中度：前三名 + 「其余」四段。
  ///
  /// 为什么是 3+1 而不是 Top 10 各占一段 —— 分类色只有 4 个槽位，
  /// 而且规范明确禁止把颜色循环复用（10 个实体用 4 个颜色，必然撞色）。
  /// 「其余」本身就是有意义的归并，不是妥协。
  List<StackSegment> _concentration() {
    final total = _items.fold<double>(0, (s, e) => s + e.gmv);
    if (total <= 0) return const [];
    final segs = <StackSegment>[];
    for (var i = 0; i < _items.length && i < 3; i++) {
      segs.add(StackSegment(
        label: _items[i].name,
        value: _items[i].gmv,
        colorIndex: i,
        detail: money(_items[i].gmv),
      ));
    }
    final rest = total - segs.fold<double>(0, (s, e) => s + e.value);
    if (_items.length > 3 && rest > 0) {
      segs.add(StackSegment(
        label: '其余 ${_items.length - 3} 项',
        value: rest,
        colorIndex: 3,
        detail: money(rest),
      ));
    }
    return segs;
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
