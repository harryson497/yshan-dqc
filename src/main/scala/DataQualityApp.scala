import bean.ConstraintResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}
import constraintsbase.{CompleteConstraint, MinimumConstraint, SizeConstraint}
import verifier.{Check}

/**
 * @description: DataQualityApp
 * @author: HanYunsong
 * @create: 2021-12-07 11:46
 */
object DataQualityApp {

  case class Item(
                   id: Long,
                   name: String,
                   age: String,
                   pri: String,
                   num: Long
                 )

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(
      Item(1, "Thingy A", "1", "high", 0),
      Item(2, "Thingy B", "1", "high", 0),
      Item(3, null, "low", "2", 5),
      Item(4, "Thingy D", "2", "low", 10),
      Item(4, "Thingy D", null, "11", 10),
      Item(4, "Thingy D", "2", "fff", 10),
      Item(5, "Thingy E", "1", "high", 12)))

    val df = spark.createDataFrame(rdd)


        Check(df)
          .hasSize()
          .hasMinLength("num")
          .hasComplete("name", _ >= 0.8)
          .isContainedIn("pri", List("high","low"))
          .hasUnique(Seq("id"), a => a > 0.7 && a < 0.9)
          .hasMin("age",_>0)
          .hasMax("id",_<5)
          .hasMean("id",_>0)
          .hasSum("id",_>0)
          .hasMaxLength("pri",_>5)
          .hasMinLength("pri",_>0)
          .run()

    //    val constraint: SizeConstraint = SizeConstraint(_ > 10, "aaaa")
    //    df.agg(constraint.aggColumn)
    //    val result: ConstraintResult = constraint.fun(df.agg(constraint.aggColumn))
    //    println(result)
    //    val constraint: SizeConstraint = SizeConstraint(_ > 10, "aaaa")
    //    val constraint1 = new UniqueConstraint(Seq("name"))
    //    val constraint2 = new CompleteConstraint("name")
    //    new Check(df,Seq(constraint,constraint1,constraint2)).run()

    //    countDf.select(expected).collect().map(_.getBoolean(0)).apply(0)
    //    df.select(col("id") > 10).show()
    //    val check: Check = Check(df,Some("aaaa"))
    //
    //    val check1: Check = check.hasNumRows(_ === 5)
    //    println(check1)
    //    val check2: Check = check1.hasUniqueKey("id")
    //    println(check2)
    //    val result: CheckResult = check2.run()

    //    val check3: Check = check.isAnyOf("name", Set("Frank", "Alex"))
    //    check1.hasNumRows()
    //    check1
    //    Check(df)
    //      .hasUniqueKey("id")
    //      .hasNumRows(_ === 5)
    //      .run()

    //    val constraint: NumberOfRowsConstraint = NumberOfRowsConstraint.greaterThan(10)
    //    val result: NumberOfRowsConstraintResult = constraint.fun(df)

  }

}
