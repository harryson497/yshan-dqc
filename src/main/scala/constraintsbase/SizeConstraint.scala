package constraintsbase

import java.util.UUID

import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, ScanningConstraint}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * @description: sizeConstraint行数约束
 * @author: HanYunsong
 * @create: 2021-12-08 18:02
 */
case class SizeConstraint private(expected: Column, id: String) extends ScanningConstraint {
  override val aggColumn: Column = count(col("*")).alias(id)

  override val fun = (df: DataFrame) => {
    val actual = df.select(id).collect().map(_.getLong(0)).apply(0)
    val satisfied: Boolean = df.select(expected).collect().map(_.getBoolean(0)).apply(0)
    val expectedValue: String = expected.toString().replace(id, "行数")

    ConstraintResult(
      this.getClass.getSimpleName,
      id,
      "DataSet",
      actual.toString,
      if (satisfied) ConstraintSuccess.stringValue else ConstraintFailure.stringValue,
      expectedValue)::Nil

  }
}

object SizeConstraint {
  def apply(expected: Column => Column, id: String = UUID.randomUUID.toString): SizeConstraint = {
    new SizeConstraint(expected(new Column(id)), id)
  }
}
