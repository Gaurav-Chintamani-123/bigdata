import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min}

object Customer_Purchase_Recency_Categorization {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val customerPurchases = List(
        ("karthik", "Premium", 50, 5000),
        ("neha", "Standard", 10, 2000),
        ("priya", "Premium", 65, 8000),
        ("mohan", "Basic", 90, 1200),
        ("ajay", "Standard", 25, 3500),
        ("vijay", "Premium", 15, 7000),
        ("veer", "Basic", 75, 1500),
        ("aatish", "Standard", 45, 3000),
        ("animesh", "Premium", 20, 9000),
        ("nishad", "Basic", 80, 1100)
      ).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")


   //5.
      val df1 = customerPurchases.withColumn("category"
      ,when(col("days_since_last_purchase")<=30,"Frequent")
          .when(col("days_since_last_purchase")<=60,"Occasional")
          .when(col("days_since_last_purchase")>60,"Rare")
          .otherwise(("Unknown"))
      )
      //df1.show()

      val df2 = df1.groupBy("category","membership")
        .agg(count("membership"))

      //df2.show()

      //6.
      val df3 = df1.filter(col("category")==="Frequent" && col("membership")==="Premium")
      .agg(avg("total_purchase_amount").alias("count_of_total_purchase_amount"))
      //df3.show()

      //7.
      val df4 = df1.filter(col("category")==="Rare")
        .groupBy("membership")
        .agg(min("total_purchase_amount")
        .alias(" minimum purchase amount"))
      df4.show()


    }

}
