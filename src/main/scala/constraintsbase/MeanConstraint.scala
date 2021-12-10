package constraintsbase

import java.util.UUID

import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, NumericConstraint, ScanningConstraint}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{avg, col, max, min}
import org.apache.spark.sql.types.DoubleType

/**
 * @description: MeanConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 11:34
 */
case class MeanConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = avg(col(columnName).cast(DoubleType)).alias(id)
  override val expectedName: String = "平均值"
}

object MeanConstraint {
  def apply(columnName: String, expected: Column => Column , id: String = UUID.randomUUID.toString): MeanConstraint = {
    new MeanConstraint(columnName: String, expected(new Column(id)), id)
  }
}