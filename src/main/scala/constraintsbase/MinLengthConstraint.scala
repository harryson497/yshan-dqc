package constraintsbase

import java.util.UUID

import bean.NumericConstraint
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * @description: MinLengthConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 13:26
 */
case class MinLengthConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = min(length(col(columnName))).alias(id)
  override val expectedName: String = "列最小长度"
}

object MinLengthConstraint {
  def apply(columnName: String, expected: Column => Column , id: String = UUID.randomUUID.toString): MinLengthConstraint = {
    new MinLengthConstraint(columnName: String, expected(new Column(id)), id)
  }
}
