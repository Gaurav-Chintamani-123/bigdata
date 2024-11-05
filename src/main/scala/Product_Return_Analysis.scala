import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count}

object Product_Return_Analysis {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val products = List(
        ("Laptop", "Electronics", 120, 45),
        ("Smartphone", "Electronics", 80, 60),
        ("Tablet", "Electronics", 50, 72),
        ("Headphones", "Accessories", 110, 47),
        ("Shoes", "Clothing", 90, 55),
        ("Jacket", "Clothing", 30, 80),
        ("TV", "Electronics", 150, 40),
        ("Watch", "Accessories", 60, 65),
        ("Pants", "Clothing", 25, 75),
        ("Camera", "Electronics", 95, 58)
      ).toDF("product_name", "category", "return_count", "satisfaction_score")

      val df1 = products.withColumn("return rate"
      ,when(col("return_count")>100 && col("satisfaction_score")<50,"High Rate Return")
          .when(col("return_count").between(50,100) && col("satisfaction_score").between(50,70),"Moderate Return Rate")
          .otherwise("Low Return Rate")
      )
      df1.show()

      val df2 = df1.groupBy("category","return rate")
        .agg(count("product_name"))
      df2.show()

    }

}
