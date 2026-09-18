import 'package:intl/intl.dart';

/// 数字格式化。规则与 HTML 看板一致，保证两个展示端读数相同。

final _thousands = NumberFormat('#,##0');

/// 紧凑格式：1,284 / 12.9K / 4.20M / 1.20B
String compact(num? n) {
  if (n == null) return '—';
  final a = n.abs();
  if (a >= 1e9) return '${(n / 1e9).toStringAsFixed(2)}B';
  if (a >= 1e6) return '${(n / 1e6).toStringAsFixed(2)}M';
  if (a >= 1e4) return '${(n / 1e3).toStringAsFixed(1)}K';
  return _thousands.format(n);
}

/// 金额格式：$1.20M
String money(num? n) => n == null ? '—' : '\$${compact(n)}';

/// 完整千分位格式：1,284,392
String full(num? n) => n == null ? '—' : _thousands.format(n);

/// 带两位小数的金额：$786.92
String money2(num? n) => n == null ? '—' : '\$${n.toStringAsFixed(2)}';
