import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, max, min, sum, when}
import org.apache.spark.sql.expressions.Window

object LEAD_LAG_1 {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._
      import org.apache.spark.sql.functions._

      val data = List((1,"Kitkat",1000,"2021-01-01"),
        (1,"Kitkat",2000,"2021-01-02"),
          (1,"Kitkat",1000,"2021-01-03"),
          (1,"Kitkat",2000,"2021-01-04"),
          (1,"Kitkat",3000,"2021-01-05"),
          (1,"Kitkat",1000,"2021-01-06")).toDF("sno","product","revenue","date")

      val window = Window.orderBy(col("product"))
      val df = data.withColumn("price_diff",col("revenue")-lead("revenue",1).over(window))
      df.show()
    }

}
