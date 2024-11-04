import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, initcap, count, sum}

object Product_Inventory_Check {
  def main(args:Array[String]):Unit=
    {

      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val inventory = List(
        ("ProductA", 120),
        ("ProductB", 95),
        ("ProductC", 45),
        ("ProductD", 200),
        ("ProductE", 75),
        ("ProductF", 30),
        ("ProductG", 85),
        ("ProductH", 100),
        ("ProductI", 60),
        ("ProductJ", 20)
      ).toDF("product_name", "stock_quantity")

      val df1 = inventory.withColumn("stock level"
      ,when(col("stock_quantity")>100,"overstocked")
          .when(col("stock_quantity")>=50 && col("stock_quantity")<=100,"normal")
          .otherwise("low stocked")
      )
      df1.show()

      val df2 = df1.groupBy(col("stock level")).agg(sum("stock_quantity"))

      df2.show()



    }

}
