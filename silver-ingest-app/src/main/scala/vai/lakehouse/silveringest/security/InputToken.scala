package vai.lakehouse.silveringest.security

import java.io.{DataInputStream, File, FileInputStream}
import java.net.URI
import java.time.{Duration, Instant}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

/** Một token trong file, đủ để log và kiểm (không có mật khẩu của token). */
final case class TokenInfo(kind: String, service: String, owner: Option[String], renewer: Option[String], issued: Option[Long],
                           maxDate: Option[Long])

/**
 * Token để ĐỌC `INPUT_PATH` ở một cluster HDFS khác (VTNET: landing EBS ở HKH, job chạy ở HLA với keytab của HLA). Hai cluster có hai KDC khác
 * nhau nên job không xin được token của cluster kia: token được lấy BẰNG TAY ở cluster đó (`hdfs fetchdt`, docs/silver-ingest/VTNET-EBS-HKH.md),
 * đặt vào một Secret, mount vào driver và executor.
 *
 *  - Executor: nạp file qua `HADOOP_TOKEN_FILE_LOCATION` (UGI tự nạp khi tạo login user, executor không đăng nhập keytab).
 *  - Driver: đăng nhập keytab (`spark.kerberos.keytab`) THAY login user và bỏ token trong file (Hadoop `loginUserFromKeytab`), nên job tự nạp file
 *    `INPUT_TOKEN_FILE` vào user hiện tại trước khi dựng Spark. Spark chỉ thêm token của nó vào sau, không xoá token này.
 *
 * Hạn thật của token (renew-interval của server, Hadoop mặc định 24 giờ, mỗi lần `fetchdt --renew` lùi thêm) KHÔNG nằm trong token: job chỉ
 * biết lúc cấp và hạn tối đa (max-lifetime). Dừng ngay (lỗi cấu hình) khi: không đọc được file, không có token HDFS cho đúng cluster của
 * `INPUT_PATH` (so bằng cùng hàm `SecurityUtil` mà DFSClient dùng để tìm token, nên bắt được lệch `hadoop.security.token.service.use_ip`), hoặc
 * đã quá hạn tối đa. Cảnh báo khi token đã cấp lâu hơn [[WarnAge]] (có thể đã hết hạn nếu không được renew).
 */
object InputToken {

  val FileEnv = "INPUT_TOKEN_FILE"

  val HdfsKind = new Text("HDFS_DELEGATION_TOKEN")

  /** Token cấp lâu hơn chừng này mà chưa renew thì sắp hết (renew-interval mặc định 24 giờ). */
  val WarnAge: Duration = Duration.ofHours(20)

  private val SchemeAuthority = "^([A-Za-z][A-Za-z0-9+.-]*)://([^/]+)(?:/.*)?$".r

  /** Kết quả của [[install]]: token đã thêm vào user hiện tại, kèm các cảnh báo. */
  final case class Installed(file: String, tokens: Seq[TokenInfo], warnings: Seq[String])

  /**
   * Không có `INPUT_TOKEN_FILE`: `Right(None)` (đọc cùng cluster như trước). Có: đọc, kiểm, thêm vào user hiện tại. Lỗi: `Left(thông báo)`, chưa thêm
   * gì. Thông báo không chứa nội dung token.
   */
  def install(env: Map[String, String], inputPath: String, now: Long = System.currentTimeMillis()): Either[String, Option[Installed]] =
    env.get(FileEnv).map(_.trim).filter(_.nonEmpty) match {
      case None => Right(None)
      case Some(file) =>
        for {
          creds <- read(file)
          checked <- check(creds, inputPath, now)
        } yield {
          UserGroupInformation.getCurrentUser.addCredentials(creds)
          Some(Installed(file, checked._1, checked._2))
        }
    }

  def read(file: String): Either[String, Credentials] = {
    val f = new File(file)
    if (!f.isFile) Left(s"$FileEnv: no file at $file (is the Secret of the token mounted?)")
    else {
      val in = new DataInputStream(new FileInputStream(f))
      try {
        val creds = new Credentials()
        creds.readTokenStorageStream(in)
        Right(creds)
      } catch {
        case e: java.io.IOException =>
          Left(s"$FileEnv: $file is not a Hadoop token file (${e.getClass.getSimpleName}): fetch the token again (hdfs fetchdt)")
      } finally in.close()
    }
  }

  /** Service mà DFSClient sẽ tìm token cho `inputPath` (host:port hoặc ip:port tuỳ `use_ip`). */
  def expectedService(inputPath: String): Either[String, String] = inputPath match {
    case SchemeAuthority(scheme, authority) if scheme.equalsIgnoreCase("hdfs") =>
      Try(SecurityUtil.buildTokenService(new URI(s"hdfs://$authority")).toString).toEither.left.map { e =>
        s"$FileEnv: cannot build the token service of $authority (${e.getClass.getSimpleName}: ${e.getMessage})"
      }
    case _ => Left(s"$FileEnv: only for an hdfs:// INPUT_PATH on another cluster, not ${inputPath.takeWhile(_ != ':')}")
  }

  def infos(creds: Credentials): Seq[TokenInfo] =
    creds.getAllTokens.asScala.toSeq.map { token =>
      val ident = Try(token.decodeIdentifier()).toOption.collect { case d: AbstractDelegationTokenIdentifier => d }
      TokenInfo(token.getKind.toString, token.getService.toString, ident.map(_.getOwner.toString), ident.map(_.getRenewer.toString),
        ident.map(_.getIssueDate).filter(_ > 0), ident.map(_.getMaxDate).filter(_ > 0))
    }.sortBy(t => (t.kind, t.service))

  /** Kiểm token đọc `inputPath`: có, chưa quá hạn tối đa. Trả về (mọi token, cảnh báo). */
  def check(creds: Credentials, inputPath: String, now: Long): Either[String, (Seq[TokenInfo], Seq[String])] =
    expectedService(inputPath).flatMap { service =>
      val all = infos(creds)
      all.find(t => t.kind == HdfsKind.toString && t.service == service) match {
        case None =>
          Left(s"$FileEnv: no HDFS token for $service, the cluster of INPUT_PATH (the file has: " +
            s"${if (all.isEmpty) "no token" else all.map(t => s"${t.kind} ${t.service}").mkString(", ")}). Fetch the token again; if the " +
            s"file has the same host written as an IP (or the opposite), add an alias: hadoop dtutil edit -service <in the file> -alias $service <file>")
        case Some(hdfs) =>
          hdfs.maxDate match {
            case Some(max) if max <= now =>
              Left(s"$FileEnv: the HDFS token for $service passed its max lifetime at ${Instant.ofEpochMilli(max)}: fetch a new one (hdfs fetchdt)")
            case _ =>
              val age = hdfs.issued.map(now - _)
              Right(all -> age.filter(_ >= WarnAge.toMillis).map { a =>
                s"the HDFS token for $service was issued ${a / 3600000} h ago: unless it was renewed (hdfs fetchdt --renew), it ends after the " +
                  "renew interval of the cluster (default 24 h) and the read fails with 'token ... is expired'"
              }.toSeq)
          }
      }
    }

  /** Dòng log của một token (không có mật khẩu). */
  def describe(t: TokenInfo, now: Long): String = {
    def at(ms: Option[Long]): String = ms.fold("?")(m => Instant.ofEpochMilli(m).toString)
    val age = t.issued.fold("")(i => s", ${(now - i) / 60000} min ago")
    s"${t.kind} ${t.service} (owner ${t.owner.getOrElse("?")}, renewer ${t.renewer.getOrElse("?")}, issued ${at(t.issued)}$age, " +
      s"max lifetime ${at(t.maxDate)})"
  }
}
