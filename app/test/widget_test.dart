import 'package:flutter_test/flutter_test.dart';
import 'package:shop_insight_app/models/metrics.dart';
import 'package:shop_insight_app/utils/format.dart';

/// 单元测试。
///
/// 只测纯函数和反序列化 —— 不测 Widget 树，因为页面一挂载就会发 HTTP 请求，
/// 测试环境里没有后端，会超时失败。要测 UI 需要注入 mock 的 http client，
/// 那是下一步的事。
void main() {
  group('数字格式化', () {
    test('紧凑格式按量级切换单位', () {
      expect(compact(1284), '1,284');
      expect(compact(12934), '12.9K');
      expect(compact(4200000), '4.20M');
      expect(compact(1200000000), '1.20B');
      expect(compact(null), '—');
    });

    test('完整格式带千分位', () {
      expect(full(110550743), '110,550,743');
      expect(full(null), '—');
    });

    test('金额格式', () {
      // K 量级保留 1 位小数、M/B 量级保留 2 位 —— 与 HTML 看板的格式化逻辑一致
      expect(money(801082.78), r'$801.1K');
      expect(money(3197295.2), r'$3.20M');
      expect(money2(786.9231), r'$786.92');
    });
  });

  group('反序列化', () {
    test('数值字段兼容 int 与 double', () {
      // gmv 可能是 int（如 100）也可能是 double（如 100.5），都要能解析
      final a = WindowMetric.fromJson({
        'window_start': '2019-11-01 06:44:00',
        'pv': 1138,
        'uv': 816,
        'purchase_cnt': 23,
        'gmv': 9281, // int
      });
      expect(a.gmv, 9281.0);
      expect(a.pv, 1138);

      final b = WindowMetric.fromJson({
        'window_start': '2019-11-01 06:44:00',
        'pv': 1,
        'uv': 1,
        'purchase_cnt': 1,
        'gmv': 100.55, // double
      });
      expect(b.gmv, closeTo(100.55, 0.001));
    });

    test('缺字段不崩，回退到 0 或空串', () {
      final m = WindowMetric.fromJson({});
      expect(m.pv, 0);
      expect(m.gmv, 0.0);
      expect(m.windowStart, '');
    });

    test('客单价在零购买时返回 0（不除零）', () {
      const noPurchase = WindowMetric(
        windowStart: 'x',
        pv: 100,
        uv: 50,
        purchaseCnt: 0,
        gmv: 0,
      );
      expect(noPurchase.avgOrderValue, 0);
    });

    test('排行项按指定字段取显示名', () {
      final brand = RankingItem.fromJson(
        {'brand': 'apple', 'pv': 26914, 'purchase_cnt': 1018, 'gmv': 801082.78},
        'brand',
      );
      expect(brand.name, 'apple');
      expect(brand.avgOrderValue, closeTo(786.92, 0.01));

      final cat = RankingItem.fromJson(
        {'category_id': 2053013555631882655, 'pv': 1, 'purchase_cnt': 1, 'gmv': 1.0},
        'category_id',
      );
      // 类目 ID 是 2.18e18 量级的大整数，转字符串不能丢精度
      expect(cat.name, '2053013555631882655');
    });
  });
}
