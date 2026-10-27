package vai.lakehouse.keyprefix

import java.util.Locale

/**
 * Dựng [[PathKeySource]] (key theo đường dẫn) từ cùng các key logic của [[PrefixSourceFactory]]: `source` BẮT BUỘC `dak`, và
 * `dak.addr`, `dak.tokenUrl`, `dak.clientId`, `dak.clientSecret`, `dak.allowInsecureHttp`, `dak.tag`, `dak.caller` như
 * [[ColumnKeySourceFactory]]. Không cache: một job chỉ hỏi một đường dẫn một lần.
 */
object PathKeySourceFactory {

  import PrefixSourceFactory.Keys

  /** Ném `IllegalArgumentException` (không lộ giá trị nhạy cảm) nếu cấu hình sai hoặc thiếu. */
  def create(cfg: ConfigSource): PathKeySource = {
    val source = cfg.get(Keys.Source).map(_.toLowerCase(Locale.ROOT))
    require(source.contains(PrefixSourceFactory.DakSource),
      s"Path keys are only available from DAK: set ${cfg.describe(Keys.Source)}=${PrefixSourceFactory.DakSource} " +
        s"(got: ${source.getOrElse("<not set>")})")
    val dak = DakSettings.config(cfg)
    new DakPathKeySource(dak, DakSettings.tokens(cfg, dak))
  }
}
