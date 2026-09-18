import 'dart:convert';

import 'package:http/http.dart' as http;

import '../config/api_config.dart';
import '../models/metrics.dart';

/// 后端分析接口的封装。
///
/// 对应 Spring Boot 的 `AnalyticsController`，四个端点：
///   GET /api/overview
///   GET /api/brand-ranking
///   GET /api/category-ranking
///   GET /api/product-ranking
class AnalyticsApi {
  /// 统一的请求方法：拼 URL、发请求、检查状态码、解码 JSON。
  static Future<List<dynamic>> _get(String path, Map<String, String> query) async {
    final uri = Uri.parse('${ApiConfig.baseUrl}$path').replace(queryParameters: query);
    final resp = await http.get(uri).timeout(ApiConfig.timeout);

    if (resp.statusCode != 200) {
      throw Exception('$path 返回 HTTP ${resp.statusCode}');
    }
    return jsonDecode(utf8.decode(resp.bodyBytes)) as List<dynamic>;
  }

  /// 实时总览：最近 N 个窗口的全局 PV / UV / 购买数 / 成交额。
  static Future<List<WindowMetric>> overview({int limit = 60}) async {
    final rows = await _get('/api/overview', {'limit': '$limit'});
    return rows.map((e) => WindowMetric.fromJson(e as Map<String, dynamic>)).toList();
  }

  /// 品牌成交额排行。
  static Future<List<RankingItem>> brandRanking({int limit = 20}) async {
    final rows = await _get('/api/brand-ranking', {'limit': '$limit'});
    return rows.map((e) => RankingItem.fromJson(e as Map<String, dynamic>, 'brand')).toList();
  }

  /// 类目成交额排行。
  static Future<List<RankingItem>> categoryRanking({int limit = 20}) async {
    final rows = await _get('/api/category-ranking', {'limit': '$limit'});
    return rows
        .map((e) => RankingItem.fromJson(e as Map<String, dynamic>, 'category_id'))
        .toList();
  }

  /// 商品成交额排行。
  static Future<List<RankingItem>> productRanking({int limit = 20}) async {
    final rows = await _get('/api/product-ranking', {'limit': '$limit'});
    return rows.map((e) => RankingItem.fromJson(e as Map<String, dynamic>, 'product_id')).toList();
  }

  // ── 以下三个来自 Spark 离线计算 ────────────────────────────────────────

  /// 每日转化漏斗。
  static Future<List<FunnelRow>> funnel({int limit = 30}) async {
    final rows = await _get('/api/funnel', {'limit': '$limit'});
    return rows.map((e) => FunnelRow.fromJson(e as Map<String, dynamic>)).toList();
  }

  /// 留存曲线（按批次大小加权的平均留存率）。
  static Future<List<RetentionPoint>> retention({int maxDay = 30}) async {
    final rows = await _get('/api/retention', {'maxDay': '$maxDay'});
    return rows.map((e) => RetentionPoint.fromJson(e as Map<String, dynamic>)).toList();
  }

  /// RFM 用户分群。
  static Future<List<UserSegment>> userSegments() async {
    final rows = await _get('/api/user-segments', const {});
    return rows.map((e) => UserSegment.fromJson(e as Map<String, dynamic>)).toList();
  }
}
