package constraintsbase

import java.util.UUID

import bean.NumericConstraint
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * @description: SumConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 13:21
 */
case class SumConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = sum(col(columnName).cast(DoubleType)).alias(id)
  override val expectedName: String = "列总和"
}

object SumConstraint {
  def apply(columnName: String, expected: Column => Column, id: String = UUID.randomUUID.toString): SumConstraint = {
    new SumConstraint(columnName: String, expected(new Column(id)), id)
  }
}


