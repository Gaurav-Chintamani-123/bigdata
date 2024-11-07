import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Product_Sales_Analysis {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val productSales = List(
        ("Product1", 250000, 5),
        ("Product2", 150000, 8),
        ("Product3", 50000, 20),
        ("Product4", 120000, 10),
        ("Product5", 300000, 7),
        ("Product6", 60000, 18),
        ("Product7", 180000, 9),
        ("Product8", 45000, 25),
        ("Product9", 70000, 15),
        ("Product10", 10000, 30)
      ).toDF("product_name", "total_sales", "discount")

      val df1 = productSales.withColumn("category"
      ,when(col("total_sales")>200000 && col("discount")<10,"top seller")
          .when(col("total_sales").between(100000,200000),"moderate seller")
          .otherwise("low seller")
      )
      df1.show()

      //1.
     val df2 = df1.groupBy("category")
        .agg(count("product_name").alias("count"))
      df2.show()

      //2.
      val df3 = df1.filter(col("category")==="top seller")
        .agg(max("total_sales").alias("max_sales"))
      df3.show()


      val df4 = df1.filter(col("category")==="moderate seller")
        .agg(min("discount").alias("min_discount"))
      df4.show()

      val df5 = df1.filter(col("category")==="low seller" &&
      col("total_sales")<50000 &&
      col("discount")>15)
      df5.show()

    }

}
