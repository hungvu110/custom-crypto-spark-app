package vai.lakehouse.hdfs

import java.util.{Map => JMap}

import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin._

/**
 * Fallback nếu ServiceLoader thua cuộc đua classpath (mục 15 của
 * SPARK_HDFS_WIRE_ENCRYPTION_TASK.md): `tokenKindMap` là static và bị cache
 * ngay lần decode đầu tiên. Nếu thứ gì đó trong Spark gọi
 * `Token.decodeIdentifier()` trước khi classloader người dùng sẵn sàng thì
 * ServiceLoader-based patch không kịp thắng.
 *
 * Plugin này chạy rất sớm trên cả driver lẫn executor, không phụ thuộc thứ tự
 * classpath, và ghi đè trực tiếp bằng reflection vào field static của Token.
 *
 * KHÔNG bật mặc định. Chỉ bật bằng `spark.plugins` khi mục 14 (verify)
 * cho thấy patch qua ServiceLoader không hoạt động.
 */
class BlockTokenFixPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin     = new BlockTokenFixInjector
  override def executorPlugin(): ExecutorPlugin = new BlockTokenFixInjector
}

private class BlockTokenFixInjector extends DriverPlugin with ExecutorPlugin {

  private def inject(): Unit = {
    try {
      // Ép populate tokenKindMap trước khi ghi đè.
      try new Token[TokenIdentifier]().decodeIdentifier() catch { case _: Throwable => }

      val field = classOf[Token[_]].getDeclaredField("tokenKindMap")
      field.setAccessible(true)
      val map = field.get(null).asInstanceOf[JMap[Text, Class[_]]]

      // BlockTokenIdentifier.KIND_NAME là package-private trong Hadoop 3.3.4,
      // không compile được từ package này -> lấy kind qua getKind() public.
      val kind = new BlockTokenIdentifier().getKind()

      if (map != null) {
        map.put(kind, classOf[LenientBlockTokenIdentifier])
        // scalastyle:off println
        println("[BlockTokenFixPlugin] Đã inject LenientBlockTokenIdentifier vào tokenKindMap")
        // scalastyle:on println
      }
    } catch {
      case t: Throwable =>
        // scalastyle:off println
        println(s"[BlockTokenFixPlugin] Inject thất bại: ${t.getClass.getName}: ${t.getMessage}")
        // scalastyle:on println
    }
  }

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    inject()
    java.util.Collections.emptyMap()
  }

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = inject()

  // DriverPlugin và ExecutorPlugin đều khai báo shutdown(): Unit -> trùng chữ
  // ký, phải override tường minh để giải quyết xung đột kim cương.
  override def shutdown(): Unit = ()
}
