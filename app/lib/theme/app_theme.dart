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
  final Color series1; // 序列色（本应用所有图表都是单序列，只需一个）

  const AppPalette({
    required this.surface,
    required this.plane,
    required this.ink,
    required this.ink2,
    required this.muted,
    required this.grid,
    required this.series1,
  });

  static const light = AppPalette(
    surface: Color(0xFFFCFCFB),
    plane: Color(0xFFF9F9F7),
    ink: Color(0xFF0B0B0B),
    ink2: Color(0xFF52514E),
    muted: Color(0xFF898781),
    grid: Color(0xFFE1E0D9),
    series1: Color(0xFF2A78D6),
  );

  static const dark = AppPalette(
    surface: Color(0xFF1A1A19),
    plane: Color(0xFF0D0D0D),
    ink: Color(0xFFFFFFFF),
    ink2: Color(0xFFC3C2B7),
    muted: Color(0xFF898781),
    grid: Color(0xFF2C2C2A),
    series1: Color(0xFF3987E5),
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
