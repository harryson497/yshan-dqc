package verifier

import bean.Constraint
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import constraintsbase.{CompleteConstraint, ComplianceConsttraint, MaxLengthConstraint, MaximumConstraint, MeanConstraint, MinLengthConstraint, MinimumConstraint, SizeConstraint, SumConstraint}
import rulebase.UniqueConstraint
//import rulebase.GeneralScanningConstraint
import bean.Constraint.ConstraintFunction

/**
 * @description: Check
 * @author: HanYunsong
 * @create: 2021-12-08 16:20
 */
case class Check(dataFrame: DataFrame,
                 constraints: Seq[Constraint] = Seq.empty,
                 var cacheMethod: StorageLevel = Check.defaultCacheMethod
                ) {

  def addConstraint(c: Constraint): Check =
    Check(dataFrame, constraints ++ List(c), cacheMethod)

  /**
   * 行数检测
   */
  def hasSize(expected: Column => Column = _>0): Check = {
    addConstraint(Check.hasSize(expected))
  }

  /**
   * 完整性检测
   */
  def hasComplete(column: String, expected: Column => Column = _===1): Check = {
    addConstraint(Check.hasComplete(column, expected))
  }
  /**
   * 可塑性检测
   */
  def isContainedIn(column: String, listSet: List[Any]): Check = {
    addConstraint(Check.isContainedIn(column, listSet))
  }
  /**
   * 唯一性
   */
  def hasUnique(columnNames: Seq[String], expected:  Column => Column= _ === 1): Check = {
    addConstraint(Check.hasUnique(columnNames, expected))
  }
  /**
   * 最小值
   */
  def hasMin(columnNames: String, expected:  Column => Column= _ =!= 0): Check = {
    addConstraint(Check.hasMin(columnNames, expected))
  }
  /**
   * 最大值
   */
  def hasMax(columnNames: String, expected:  Column => Column = _ =!= 0): Check = {
    addConstraint(Check.hasMax(columnNames, expected))
  }
  /**
   * 平均值
   */
  def hasMean(columnNames: String, expected:  Column => Column= _ =!= 0): Check = {
    addConstraint(Check.hasMean(columnNames, expected))
  }
  /**
   * 列总和
   */
  def hasSum(columnNames: String, expected:  Column => Column= _ =!= 0): Check = {
    addConstraint(Check.hasSum(columnNames, expected))
  }
  /**
   * 最小长度
   */
  def hasMinLength(columnNames: String, expected:  Column => Column= _ > 0): Check = {
    addConstraint(Check.hasMinLength(columnNames, expected))
  }
  /**
   * 最大长度
   */
  def hasMaxLength(columnNames: String, expected:  Column => Column= _ > 0): Check = {
    addConstraint(Check.hasMaxLength(columnNames, expected))
  }



  def run(): Unit = {
    Runner.run(this)
  }
}

object Check {
  def hasSize(expected: Column => Column): Constraint = SizeConstraint(expected)

  def hasComplete(column: String, expected: Column => Column): Constraint = CompleteConstraint(column, expected)

  def isContainedIn(column: String, listSet: List[Any]): Constraint = ComplianceConsttraint(column,listSet)

  def hasUnique(columnNames: Seq[String], expected: Column => Column): Constraint = UniqueConstraint(columnNames,expected)

  def hasMin(columnNames: String, expected: Column => Column): Constraint = MinimumConstraint(columnNames,expected)

  def hasMax(columnNames: String, expected: Column => Column): Constraint = MaximumConstraint(columnNames,expected)

  def hasMean(columnNames: String, expected: Column => Column): Constraint = MeanConstraint(columnNames,expected)

  def hasSum(columnNames: String, expected: Column => Column): Constraint = SumConstraint(columnNames,expected)

  def hasMinLength(columnNames: String, expected: Column => Column): Constraint = MinLengthConstraint(columnNames,expected)

  def hasMaxLength(columnNames: String, expected: Column => Column): Constraint = MaxLengthConstraint(columnNames,expected)

  private val defaultCacheMethod = StorageLevel.MEMORY_ONLY
  private val MADCacheMethod = StorageLevel.MEMORY_AND_DISK

}