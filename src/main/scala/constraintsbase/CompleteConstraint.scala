package constraintsbase

import java.util.UUID

import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, ScanningConstraint}
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{Column, DataFrame}
import bean.utils.rateName

/**
 * @description: CompleteConstraint完整性约束
 * @author: HanYunsong
 * @create: 2021-12-08 19:25
 */
case class CompleteConstraint private(columnName: String, expected: Column, id: String) extends ScanningConstraint {
  override val aggColumn: Column = sum(col(columnName).isNotNull.cast(IntegerType)).alias(id)
  override val fun = (df: DataFrame) => {
    val rateColumn = rateName(id)
    val rateDF: DataFrame = df.withColumn(rateColumn, col(id) / col(countName))
    val (actual,rowCount,rate): (Long, Long, Double) = rateDF.select(id, countName, rateColumn).collect().map(a => (a.getLong(0), a.getLong(1), a.getDouble(2))).apply(0)

    val satisfied: Boolean = rateDF.select(expected).collect().map(_.getBoolean(0)).apply(0)
    val expectedValue: String = expected.toString().replace(rateColumn, "非空比例")

    ConstraintResult(
      this.getClass.getSimpleName,
      id,
      columnName,
      actual.toString,
      if (satisfied) ConstraintSuccess.stringValue else ConstraintFailure.stringValue,
      expectedValue)::Nil
  }
}

object CompleteConstraint {
  def apply(columnName: String, expected: Column => Column, id: String = UUID.randomUUID.toString): CompleteConstraint = {
    new CompleteConstraint(columnName: String, expected(new Column(rateName(id))), id)
  }
}
