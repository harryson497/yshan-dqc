package verifier

import bean.{Constraint, ConstraintResult, GroupingConstraint, ScanningConstraint}
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel
import constraintsbase.{GeneralScanningConstraint, SizeConstraint}
import bean.Constraint.ConstraintFunction

/**
 * @description: Runner
 * @author: HanYunsong
 * @create: 2021-12-08 16:19
 */
object Runner {
  def run(checks: Check) = {
    var scanningColumns: Seq[Column] = Seq.empty
    var groupingConstraint: Seq[Constraint] = Seq.empty
    var scanningConstraint: Seq[Constraint] = Seq.empty
    var scanningFun: Seq[ConstraintFunction] = Seq.empty

    checks.constraints.foreach(c => {
      c match {
        case c: ScanningConstraint =>
          scanningColumns +:= c.asInstanceOf[ScanningConstraint].aggColumn
          scanningFun +:= c.fun
        case c: GroupingConstraint => groupingConstraint +:= c
        case _ => println("约束类型判断失败")
      }
    })
    if (!groupingConstraint.isEmpty) checks.cacheMethod = StorageLevel.MEMORY_AND_DISK
    //    val persistedDf: DataFrame = dataFrame.persist(cacheMethod)
    checks.dataFrame
    GeneralScanningConstraint(scanningColumns,scanningFun)
    scanningConstraint +:= GeneralScanningConstraint(scanningColumns,scanningFun)
    (groupingConstraint ++ scanningConstraint).map(c => {
      val result: Seq[ConstraintResult] = c.fun(checks.dataFrame)
      result.foreach(println)
    })

  }

}
