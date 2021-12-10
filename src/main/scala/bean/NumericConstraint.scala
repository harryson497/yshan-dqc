package bean

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame}

/**
 * @description: NumericConstraint
 * @author: HanYunsong
 * @create: 2021-12-10 11:39
 */
abstract class NumericConstraint(columnName: String, expected: Column, id: String) extends ScanningConstraint {
  val expectedName:String
  override val fun = (df: DataFrame) => {
    val actual = df.select(col(id).cast(DoubleType)).collect().map(_.getDouble(0)).apply(0)
    val satisfied: Boolean = df.select(expected).collect().map(_.getBoolean(0)).apply(0)
    val expectedValue: String = expected.toString().replace(id, expectedName)

    ConstraintResult(
      this.getClass.getSimpleName,
      id,
      columnName,
      actual.toString,
      if (satisfied) ConstraintSuccess.stringValue else ConstraintFailure.stringValue,
      expectedValue) :: Nil
  }
}
