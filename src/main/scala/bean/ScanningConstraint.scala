package bean

import org.apache.spark.sql.Column

/**
 * @description: ScanningConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 13:58
 */
trait ScanningConstraint extends Constraint {
  val aggColumn: Column
}
