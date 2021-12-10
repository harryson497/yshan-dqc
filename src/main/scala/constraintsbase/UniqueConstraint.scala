package rulebase

import java.util.UUID

import bean.utils.rateName
import org.apache.spark.sql.{Column, DataFrame}
import bean.{ConstraintFailure, ConstraintResult, ConstraintSuccess, GroupingConstraint}
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.IntegerType

import scala.util.Try

/**
 * @description: UniqueConstraint
 * @author: HanYunsong
 * @create: 2021-12-08 18:52
 */
case class UniqueConstraint private(columnNames: Seq[String], expected: Column, id: String) extends GroupingConstraint {
  val fun = (df: DataFrame) => {
    val columns = columnNames.map(name => col(name))
    val groupCountDF: DataFrame = df.groupBy(columns: _*).count
    val aggColumns = List(sum((col("count") === 1).cast(IntegerType)).alias(keyCountName), sum(col("count")).alias(countName))
    val keyAndRowCount: DataFrame = groupCountDF.agg(aggColumns.head, aggColumns.tail: _*)
    val rateColumn = rateName(id)
    val rateDF: DataFrame = keyAndRowCount.withColumn(rateColumn, col(keyCountName) / col(countName))
    val (actual, rowCount, rate): (Long, Long, Double) = rateDF.collect().map(a => (a.getLong(0), a.getLong(1), a.getDouble(2))).apply(0)

    val satisfied: Boolean = rateDF.select(expected).collect().map(_.getBoolean(0)).apply(0)
    val expectedValue: String = expected.toString().replace(rateColumn, "唯一比例")

    ConstraintResult(
      this.getClass.getSimpleName,
      id,
      columnNames.mkString(","),
      actual.toString,
      if (satisfied) ConstraintSuccess.stringValue else ConstraintFailure.stringValue,
      expectedValue) :: Nil

  }
}

object UniqueConstraint {
  def apply(columnNames: Seq[String], expected: Column => Column , id: String = UUID.randomUUID.toString): UniqueConstraint = {
    new UniqueConstraint(columnNames, expected(new Column(rateName(id))), id)
  }
}