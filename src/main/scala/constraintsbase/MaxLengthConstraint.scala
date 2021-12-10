package constraintsbase

import java.util.UUID

import bean.NumericConstraint
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
 * @description: MaxLengthConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 13:51
 */

case class MaxLengthConstraint private(columnName: String, expected: Column, id: String) extends NumericConstraint(columnName, expected, id) {
  override val aggColumn: Column = max(length(col(columnName))).alias(id)
  override val expectedName: String = "列最大长度"
}

object MaxLengthConstraint {
  def apply(columnName: String, expected: Column => Column, id: String = UUID.randomUUID.toString): MaxLengthConstraint = {
    new MaxLengthConstraint(columnName: String, expected(new Column(id)), id)
  }
}

