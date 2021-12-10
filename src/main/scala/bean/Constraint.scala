package bean

import bean.Constraint.ConstraintFunction
import org.apache.spark.sql.{Column, DataFrame}

/**
 * @description: Constraint
 * @author: HanYunsong
 * @create: 2021-12-08 16:30
 */
trait Constraint {
  val fun: ConstraintFunction
  val countName = "dqc_rowcount"
}


object Constraint {
  type ConstraintFunction = (DataFrame) => Seq[ConstraintResult]
}




