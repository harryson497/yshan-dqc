package constraintsbase

import java.util.UUID

import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, NumericConstraint, ScanningConstraint}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * @description: MinimumConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 10:47
 */
case class MinimumConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = min(col(columnName).cast(DoubleType)).alias(id)
  override val expectedName: String = "最小值"
}

object MinimumConstraint {
  def apply(columnName: String, expected: Column => Column , id: String = UUID.randomUUID.toString): MinimumConstraint = {
    new MinimumConstraint(columnName: String, expected(new Column(id)), id)
  }
}

