import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, initcap, count}

object Student_Grade_Classification {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val students = List(
        ("karthik", 95),
        ("neha", 82),
        ("priya", 74),
        ("mohan", 91),
        ("ajay", 67),
        ("vijay", 80),
        ("veer", 85),
        ("aatish", 72),
        ("animesh", 90),
        ("nishad", 60)
      ).toDF("name", "score")

      val df1 = students.select(col("name"),col("score")
        ,when(col("score")>=90,"Excellent")
          .when(col("score")>=75 && col("score")<=89,"Good")
          .otherwise("Needs Improvement").alias("category")
      )
      df1.show()

      val df2 = df1.groupBy(col("category")).agg(count("category"))
      df2.show()
    }

}
