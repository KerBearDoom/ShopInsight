import 'package:flutter/material.dart';

/// 调色板。与 HTML 看板（`src/main/resources/static/index.html`）用同一套值，
/// 保证两个展示端视觉一致。
///
/// 这套值跑过可视化规范的验证器：浅色和深色模式**全部 PASS**
/// （亮度带、色度下限、对比度 ≥ 3:1）。
class AppPalette {
  final Color surface; // 卡片表面
  final Color plane; // 页面底色
  final Color ink; // 主文字
  final Color ink2; // 次文字
  final Color muted; // 弱化/轴标签
  final Color grid; // 网格发丝线

  /// 分类序列色，**固定顺序、不可循环**。
  ///
  /// 索引含义（切换筛选时颜色跟着实体走，不跟着排名走）：
  ///   0 → 单序列图；漏斗第一环节（浏览）；高价值客户
  ///   1 → 漏斗第二环节（加购）；潜力客户
  ///   2 → 漏斗第三环节（购买）；一般保持
  ///   3 → 流失风险
  ///
  /// 这套值是从可视化规范的参考调色板按同一顺序取的（slot 1 正好就是本项目
  /// 原本用的 series1），并**分别对两种配对模式跑过验证器**：
  ///   · 堆叠条形 4 色 / 相邻对 → 浅色深色全 PASS
  ///   · 分组柱与折线 3 色 / 相邻对 → 全 PASS
  ///   · 雷达图 3 色 / 全对 → 全 PASS
  ///
  /// ⚠️ 两处已知的 WARN，靠"图例 + 直接标注 + 悬停提示"兜底（识别不能只靠颜色）：
  ///   1. 浅色模式下 series3/4 对比度低于 3:1
  ///   2. 三色觉下 series4↔series3 的分离度落在下限带
  ///
  /// ⚠️ 不要用饼图/环形图：4 个分类色在"任意两色都可能被比较"的场景下过不了
  /// 硬门槛（橙 ↔ 黄正常视力 ΔE 只有 13.7）。部分-整体用堆叠条形图。
  static const List<Color> seriesLight = [
    Color(0xFF2A78D6), // 蓝
    Color(0xFFEB6834), // 橙
    Color(0xFF1BAF7A), // 青
    Color(0xFFEDA100), // 黄
  ];

  static const List<Color> seriesDark = [
    Color(0xFF3987E5),
    Color(0xFFD95926),
    Color(0xFF199E70),
    Color(0xFFC98500),
  ];

  const AppPalette({
    required this.surface,
    required this.plane,
    required this.ink,
    required this.ink2,
    required this.muted,
    required this.grid,
    required this.series,
  });

  /// 分类序列色（长度 4，固定顺序）。
  final List<Color> series;

  /// 单序列图用的主色，等价于 series[0]。
  Color get series1 => series[0];

  /// 按槽位取色。超出范围时**返回最后一个**而不是取模循环 ——
  /// 规范要求分类色不能被循环复用（循环会让两个不同实体撞色）。
  Color seriesAt(int i) => series[i.clamp(0, series.length - 1)];

  static const light = AppPalette(
    surface: Color(0xFFFCFCFB),
    plane: Color(0xFFF9F9F7),
    ink: Color(0xFF0B0B0B),
    ink2: Color(0xFF52514E),
    muted: Color(0xFF898781),
    grid: Color(0xFFE1E0D9),
    series: seriesLight,
  );

  static const dark = AppPalette(
    surface: Color(0xFF1A1A19),
    plane: Color(0xFF0D0D0D),
    ink: Color(0xFFFFFFFF),
    ink2: Color(0xFFC3C2B7),
    muted: Color(0xFF898781),
    grid: Color(0xFF2C2C2A),
    series: seriesDark,
  );

  /// 按当前亮暗模式取对应调色板。
  static AppPalette of(BuildContext context) =>
      Theme.of(context).brightness == Brightness.dark ? dark : light;
}

/// 从调色板派生 Material 主题。
ThemeData buildAppTheme(Brightness brightness) {
  final p = brightness == Brightness.dark ? AppPalette.dark : AppPalette.light;
  return ThemeData(
    useMaterial3: true,
    brightness: brightness,
    scaffoldBackgroundColor: p.plane,
    colorScheme: ColorScheme.fromSeed(
      seedColor: p.series1,
      brightness: brightness,
      surface: p.surface,
    ),
    fontFamily: 'system-ui',
    cardTheme: CardThemeData(
      color: p.surface,
      elevation: 0,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(10),
        side: BorderSide(color: p.grid),
      ),
    ),
  );
}
