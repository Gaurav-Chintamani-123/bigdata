import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, min, sum, when}
import org.apache.spark.sql.expressions.Window

object LEAD_LAG_2 {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._
      import org.apache.spark.sql.functions._

      val data = List((1,"John",1000,"01/01/2016"),
      (1,"John",2000,"02/01/2016"),
        (1,"John",1000,"03/01/2016"),
      (1,"John",2000,"04/01/2016"),
      (1,"John",3000,"05/01/2016"),
      (1,"John",1000,"06/01/2016"))toDF("id","name","salary","date")

      val window = Window.partitionBy(col("id")).orderBy(col("date"))
      val df = data.withColumn("pre_sal",lag(col("salary"),1).over(window))
        .withColumn("sal_diff",when(col("salary")>col("pre_sal"),"UP")
          .when(col("salary")<col("pre_sal"),"DOWN")
        .otherwise("NA"))
      df.show()
    }

}
