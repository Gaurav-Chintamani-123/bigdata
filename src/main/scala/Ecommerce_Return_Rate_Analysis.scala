import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Ecommerce_Return_Rate_Analysis {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val ecommerceReturn = List(
        ("Product1", 75, 25),
        ("Product2", 40, 15),
        ("Product3", 30, 5),
        ("Product4", 60, 18),
        ("Product5", 100, 30),
        ("Product6", 45, 10),
        ("Product7", 80, 22),
        ("Product8", 35, 8),
        ("Product9", 25, 3),
        ("Product10", 90, 12)
      ).toDF("product_name", "sale_price", "return_rate")

      val df1 = ecommerceReturn.withColumn("category"
      ,when(col("return_rate")>20,"high return")
          .when(col("return_rate").between(10,20),"medium return")
          .otherwise("low return")
      )
      df1.show()

      val df2 = df1.groupBy("category")
        .agg(count("product_name").alias("Count products"))
      df2.show()

      val df3 = df1.filter(col("category")==="high return")
        .agg(avg("sale_price"))
      df3.show()

      val df4 = df1.filter(col("category") === "medium return")
        .agg(max("sale_price"))
      df4.show()

      val df5 = df1.filter(col("category")==="low return" &&
      col("sale_price")<50 &&
      col("return_rate")<5)
      df5.show()
    }

}
