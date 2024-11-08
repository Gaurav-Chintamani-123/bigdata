import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, col, count, max, min, sum, when}

object Student_Graduation_Prediction {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val students = List(
        ("Student1", 70, 45, 60, 65, 75),
        ("Student2", 80, 55, 58, 62, 67),
        ("Student3", 65, 30, 45, 70, 55),
        ("Student4", 90, 85, 80, 78, 76),
        ("Student5", 72, 40, 50, 48, 52),
        ("Student6", 88, 60, 72, 70, 68),
        ("Student7", 74, 48, 62, 66, 70),
        ("Student8", 82, 56, 64, 60, 66),
        ("Student9", 78, 50, 48, 58, 55),
        ("Student10", 68, 35, 42, 52, 45)
      ).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score",
        "history_score")

      val average_score = students.withColumn("avg_score",
          (col("math_score")
            + col("science_score")
            + col("english_score")
            + col("history_score")) / 4)
      average_score.show()

      val df1 = average_score.withColumn("category"
      ,when(col("attendance_percentage")<70 && col("avg_score")<50,"at-risk")
          .when(col("attendance_percentage").between(75,85),"moderate risk")
          .otherwise("low risk")
      )
      df1.show()

      val df2 = df1.groupBy("category").agg(count("student_id"))
      df2.show()

      val df3 = df2.filter(col("category")==="at-risk")
        .agg(avg("avg_score"))
      df3.show()

      val df4 = df3.filter(col("category") === "moderate risk")
        .filter(array(col("math_score"), col("science_score"), col("english_score"), col("history_score"))

    }

}
