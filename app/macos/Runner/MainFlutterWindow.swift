import Cocoa
import FlutterMacOS

class MainFlutterWindow: NSWindow {
  override func awakeFromNib() {
    let flutterViewController = FlutterViewController()
    let windowFrame = self.frame
    self.contentViewController = flutterViewController
    self.setFrame(windowFrame, display: true)

    // 初始窗口尺寸。模板默认是 800×600（写死在 MainMenu.xib 的 contentRect 里），
    // 而 UI 在宽度 < 900 时会从左侧 NavigationRail 切换成底部导航栏 ——
    // 桌面端应该看到侧边栏，所以把窗口开大，并把最小宽度锁在 900，
    // 这样用户把窗口拖窄也不会退化成移动端布局。
    self.setContentSize(NSSize(width: 1280, height: 800))
    self.contentMinSize = NSSize(width: 900, height: 640)

    RegisterGeneratedPlugins(registry: flutterViewController)

    super.awakeFromNib()
  }
}
