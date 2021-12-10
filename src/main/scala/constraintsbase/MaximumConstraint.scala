package constraintsbase

import java.util.UUID

import bean.Constraint.ConstraintFunction
import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, NumericConstraint, ScanningConstraint}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * @description: MaximumConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 11:13
 */
case class MaximumConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = max(col(columnName).cast(DoubleType)).alias(id)
  override val expectedName: String = "最大值"
}

object MaximumConstraint {
  def apply(columnName: String, expected: Column => Column, id: String = UUID.randomUUID.toString): MaximumConstraint = {
    new MaximumConstraint(columnName: String, expected(new Column(id)), id)
  }
}

