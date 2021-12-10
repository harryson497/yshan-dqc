package constraintsbase

import bean.{Constraint, ConstraintResult}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Column, DataFrame}
import bean.Constraint.ConstraintFunction

/**
 * @description: GeneralScanningConstraint
 * @author: HanYunsong
 * @create: 2021-12-08 18:18
 */
case class GeneralScanningConstraint private(aggColumn: Seq[Column], scanningFun: Seq[ConstraintFunction]) extends Constraint  {
  override val fun = (df: DataFrame) => {
    val generalScanningDF = df.agg(count(col("*")).alias("dqc_rowcount"), aggColumn: _*)
    val results: Seq[ConstraintResult] = scanningFun.map(_ (generalScanningDF).head)
    results
  }
}
