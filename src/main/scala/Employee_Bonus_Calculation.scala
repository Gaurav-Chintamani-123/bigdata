import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, sum}


object Employee_Bonus_Calculation {
  def main(args:Array[String]):Unit=
    {

      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", "Sales", 85),
        ("neha", "Marketing", 78),
        ("priya", "IT", 90),
        ("mohan", "Finance", 65),
        ("ajay", "Sales", 55),
        ("vijay", "Marketing", 82),
        ("veer", "HR", 72),
        ("aatish", "Sales", 88),
        ("animesh", "Finance", 95),
        ("nishad", "IT", 60)).toDF("name", "department", "performance_score")

      val df1 = employees.withColumn("bonus",
        when((col("department").isin("Sales", "Marketing") && col("performance_score") > 80), 0.20)
          .when(col("performance_score") > 70, 0.15)
          .otherwise(0.0) )
      df1.show()

      val df2 =df1.groupBy("department").agg(sum(col("bonus")).alias("total_bonus"))
      df2.show()

    }

}
