import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Employee_Salary_Band_and_Performance_Classification {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", "IT", 110000, 12, 88),
        ("neha", "Finance", 75000, 8, 70),
        ("priya", "IT", 50000, 5, 65),
        ("mohan", "HR", 120000, 15, 92),
        ("ajay", "IT", 45000, 3, 50),
        ("vijay", "Finance", 80000, 7, 78),
        ("veer", "Marketing", 95000, 6, 85),
        ("aatish", "HR", 100000, 9, 82),
        ("animesh", "Finance", 105000, 11, 88),
        ("nishad", "IT", 30000, 2, 55)
      ).toDF("name", "department", "salary", "experience", "performance_score")

      //10.
      val df1 = employees.withColumn("salary band"
      ,when(col("salary")>100000 && col("experience")>10,"Senior")
          .when(col("salary").between(50000,100000) && col("experience").between(5,10),"Mid-level")
            .otherwise("Junior")
      )
       df1.show()

      val df2 = df1.groupBy("department","salary band")
        .agg(count(col("salary band")).alias("count"))
     df2.show()

      //11.
      val df3 = df1.groupBy("salary band")
      .agg(avg("performance_score").alias("avg_performance"))
        .filter(col("avg_performance")>80)
      df3.show()

      //12.
      val df4 = df1.filter(col("salary band")==="Mid level" &&
      col("performance_score")>85 &&
      col("experience")>7)
      df4.show()


    }

}
