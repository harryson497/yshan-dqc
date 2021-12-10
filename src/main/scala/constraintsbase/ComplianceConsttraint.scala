package constraintsbase

import java.util.UUID

import bean.Constraint.ConstraintFunction
import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, ScanningConstraint}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.IntegerType

/**
 * @description: ComplianceConsttraint 非空值所属集合
 * @author: HanYunsong
 * @create: 2021-12-09 17:01
 */
case class ComplianceConsttraint private(columnName: String, listSet: List[Any], id: String = UUID.randomUUID.toString) extends ScanningConstraint {
  override val aggColumn: Column = sum((col(columnName).isin(listSet: _*) && col(columnName).isNotNull).cast(IntegerType)).alias(id)
  override val fun = (df: DataFrame) => {
    val rowCount = df.select(countName).collect().map(_.getLong(0)).apply(0)
    val actual = df.select(id).collect().map(_.getLong(0)).apply(0)
    val satisfied: Boolean = (rowCount == actual)
    val expectedValue: String = s"(列的值包含在集合[${listSet.mkString(",")}]中)"

    Seq(ConstraintResult(
      this.getClass.getSimpleName,
      id,
      columnName,
      actual.toString,
      if (satisfied) ConstraintSuccess.stringValue else ConstraintFailure.stringValue,
      expectedValue))
  }
}
