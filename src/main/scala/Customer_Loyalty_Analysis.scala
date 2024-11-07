import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Customer_Loyalty_Analysis {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val customerLoyalty = List(
        ("Customer1", 25, 700),
        ("Customer2", 15, 400),
        ("Customer3", 5, 50),
        ("Customer4", 18, 450),
        ("Customer5", 22, 600),
        ("Customer6", 2, 80),
        ("Customer7", 12, 300),
        ("Customer8", 6, 150),
        ("Customer9", 10, 200),
        ("Customer10", 1, 90)
      ).toDF("customer_name", "purchase_frequency", "average_spending")

      val df1 = customerLoyalty.withColumn("category"
      ,when(col("purchase_frequency")>20 && col("average_spending")>500,"highly loyal")
          .when(col("purchase_frequency").between(10,20),"moderately loyal")
          .otherwise("low loyal")
      )
      print("Print df1")
      df1.show()

      //4.
      val df2 = df1.groupBy("category").agg(count("customer_name").alias("count"))
      print("Print df2")
      df2.show()

      //5.
      val df3 = df1.filter(col("category")==="highly loyal")
        .agg(avg("average_spending").alias("avg spending highly loyal"))
      print("Print df3")
      df3.show()

      val df4 = df1.filter(col("category")==="moderately loyal")
        .agg(min("average_spending").alias("min spending mod loyal"))
      print("Print df4")
      df4.show()

      //6.
      val df5 = df1.filter(col("category")==="low loyal" &&
        col("average_spending")<100 &&
        col("purchase_frequency")<5)
      print("Print df5")
      df5.show()



    }

}
