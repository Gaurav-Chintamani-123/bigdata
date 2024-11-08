import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Employee_Productivity_Scoring {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val employeeProductivity = List(
        ("Emp1", 85, 6),
        ("Emp2", 75, 4),
        ("Emp3", 40, 1),
        ("Emp4", 78, 5),
        ("Emp5", 90, 7),
        ("Emp6", 55, 3),
        ("Emp7", 80, 5),
        ("Emp8", 42, 2),
        ("Emp9", 30, 1),
        ("Emp10", 68, 4)
      ).toDF("employee_id", "productivity_score", "project_count")

      val df1 = employeeProductivity.withColumn("category"
      ,when(col("productivity_score")>80 && col("project_count")>5,"high performer")
          .when(col("productivity_score").between(60,80),"average performer")
          .otherwise("low performer")
      )
      df1.show()

      //10.
      val df2 = df1.groupBy("category").agg(count("employee_id").alias("count"))
      df2.show()

      //11
      val df3 = df1.filter(col("category")==="high performer")
        .agg(avg("productivity_score"))
      df3.show()

      //11
      val df4 = df1.filter(col("category")==="average performer")
        .agg(min("productivity_score"))
      df4.show()

      //12
      val df5 = df1.filter(col("category")==="low performer" &&
      col("productivity_score")<50 &&
      col("project_count")<2)
      df5.show()
    }

}
