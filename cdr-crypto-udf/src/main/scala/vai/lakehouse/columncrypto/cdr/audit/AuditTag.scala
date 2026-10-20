package vai.lakehouse.columncrypto.cdr.audit

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * Đánh dấu một lần dùng `cdr_encrypt`/`cdr_decrypt` trong plan để [[CdrAuditListener]] ghép được với SQL execution: tính toán và sinh
 * code y hệt `child`; `sql` (Spark dùng để tự đặt tên cột) giữ nguyên của `child` nên tên cột kết quả không đổi; chỉ `toString` (Spark
 * dùng khi in plan, kể cả `physicalPlanDescription` của `SparkListenerSQLExecutionStart`) thêm `[cdr_audit:<usageId>]`.
 * Hành vi này được giữ bằng test hồi quy `SparkAuditContractSpec`.
 */
case class AuditTag(child: Expression, usageId: String) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def eval(input: InternalRow): Any = child.eval(input)
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new IllegalStateException("AuditTag.genCode delegates to its child and never calls doGenCode")
  override def sql: String = child.sql
  override def toString: String = s"$child[${AuditTag.Marker}$usageId]"
  override protected def withNewChildInternal(newChild: Expression): AuditTag = copy(child = newChild)
}

object AuditTag {

  val Marker = "cdr_audit:"

  // `u` + 12 ký tự hex: không trùng được với exprId của Spark (`name#123`).
  private val Pattern = "cdr_audit:(u[0-9a-f]{12})".r

  def newUsageId(): String = f"u${ThreadLocalRandom.current().nextLong() & 0xFFFFFFFFFFFFL}%012x"

  /** Các usageId có trong chuỗi plan, theo thứ tự xuất hiện, không trùng. */
  def usageIdsIn(planText: String): Seq[String] = Pattern.findAllMatchIn(planText).map(_.group(1)).toSeq.distinct
}
