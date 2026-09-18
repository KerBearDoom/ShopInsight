/// 后端 API 配置。
class ApiConfig {
  /// Flutter Web 跑在浏览器里，`localhost` 就是宿主机本身，直接写即可。
  ///
  /// ⚠️ 以后做 Android 端时要改这里 —— 模拟器里的 `localhost` 指模拟器自己，
  /// 必须用 `10.0.2.2`（Android 模拟器访问宿主机的专用地址）；
  /// 真机则要填 Mac 的局域网 IP。所以这个值要按平台判断，不能写死。
  static const String baseUrl = 'http://localhost:8080';

  /// 请求超时。数据量不大，5 秒足够。
  static const Duration timeout = Duration(seconds: 5);
}
