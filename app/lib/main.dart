import 'package:flutter/material.dart';

import 'api/analytics_api.dart';
import 'pages/insight_page.dart';
import 'pages/overview_page.dart';
import 'pages/ranking_page.dart';
import 'theme/app_theme.dart';

void main() => runApp(const ShopInsightApp());

class ShopInsightApp extends StatelessWidget {
  const ShopInsightApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'ShopInsight',
      debugShowCheckedModeBanner: false,
      theme: buildAppTheme(Brightness.light),
      darkTheme: buildAppTheme(Brightness.dark),
      // 跟随系统亮暗模式。调色板的深色值是**独立选定**的，
      // 不是把浅色自动反转 —— 两套都跑过对比度验证。
      themeMode: ThemeMode.system,
      home: const HomeShell(),
    );
  }
}

/// 应用外壳：负责导航 + 承载四个页面。
///
/// 响应式：宽屏（≥900px）用侧边 NavigationRail，窄屏用底部 NavigationBar。
/// Flutter 的好处是同一套代码，浏览器窗口拉宽拉窄会自动切换布局。
class HomeShell extends StatefulWidget {
  const HomeShell({super.key});

  @override
  State<HomeShell> createState() => _HomeShellState();
}

class _HomeShellState extends State<HomeShell> {
  int _index = 0;

  static const _destinations = [
    (icon: Icons.dashboard_outlined, selected: Icons.dashboard, label: '实时总览'),
    (icon: Icons.insights_outlined, selected: Icons.insights, label: '经营分析'),
    (icon: Icons.storefront_outlined, selected: Icons.storefront, label: '品牌排行'),
    (icon: Icons.category_outlined, selected: Icons.category, label: '类目排行'),
    (icon: Icons.inventory_2_outlined, selected: Icons.inventory_2, label: '商品排行'),
  ];

  /// 用 IndexedStack 而不是每次重建页面 —— 保住各页的滚动位置和定时器，
  /// 切回来时不用重新等一次网络请求。
  late final _pages = [
    const OverviewPage(),
    // 经营分析：展示 Spark 离线算的漏斗 / RFM 分群 / 留存
    const InsightPage(),
    RankingPage(
      title: '品牌成交额排行',
      subtitle: '品牌是数据里最接近「商家」的实体 —— 这是商家经营视角的入口',
      nameHeader: '品牌',
      fetcher: (limit) => AnalyticsApi.brandRanking(limit: limit),
    ),
    RankingPage(
      title: '类目成交额排行',
      subtitle: '按类目聚合，定位高价值品类',
      nameHeader: '类目 ID',
      fetcher: (limit) => AnalyticsApi.categoryRanking(limit: limit),
    ),
    RankingPage(
      title: '商品成交额排行',
      subtitle: '直接从 1.1 亿行明细表计算，含独立访客数',
      nameHeader: '商品 ID',
      showUv: true,
      fetcher: (limit) => AnalyticsApi.productRanking(limit: limit),
    ),
  ];

  @override
  Widget build(BuildContext context) {
    final p = AppPalette.of(context);

    return LayoutBuilder(builder: (context, constraints) {
      final wide = constraints.maxWidth >= 900;

      final body = IndexedStack(index: _index, children: _pages);

      if (wide) {
        return Scaffold(
          body: Row(children: [
            NavigationRail(
              selectedIndex: _index,
              onDestinationSelected: (i) => setState(() => _index = i),
              labelType: NavigationRailLabelType.all,
              backgroundColor: p.surface,
              destinations: [
                for (final d in _destinations)
                  NavigationRailDestination(
                    icon: Icon(d.icon),
                    selectedIcon: Icon(d.selected),
                    label: Text(d.label, style: const TextStyle(fontSize: 12)),
                  ),
              ],
            ),
            VerticalDivider(width: 1, color: p.grid),
            Expanded(child: body),
          ]),
        );
      }

      return Scaffold(
        body: body,
        bottomNavigationBar: NavigationBar(
          selectedIndex: _index,
          onDestinationSelected: (i) => setState(() => _index = i),
          destinations: [
            for (final d in _destinations)
              NavigationDestination(
                icon: Icon(d.icon),
                selectedIcon: Icon(d.selected),
                label: d.label,
              ),
          ],
        ),
      );
    });
  }
}
