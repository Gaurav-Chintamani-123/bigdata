import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Electricity_Consumption_and_Rate_Assignment {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val electricityUsage = List(
        ("House1", 550, 250),
        ("House2", 400, 180),
        ("House3", 150, 50),
        ("House4", 500, 200),
        ("House5", 600, 220),
        ("House6", 350, 120),
        ("House7", 100, 30),
        ("House8", 480, 190),
        ("House9", 220, 105),
        ("House10", 150, 60)
      ).toDF("household", "kwh_usage", "total_bill")

      //7.
      val df1 = electricityUsage.withColumn("usage category"
      ,when(col("kwh_usage")>500 && col("total_bill")>200,"high usage")
          .when(col("kwh_usage").between(200,500) && col("total_bill").between(100,200),"medium usage")
          .otherwise("low usage")
      )
      df1.show()

      val df2 = df1.groupBy("usage category")
        .agg(count("household").alias("total number of households "))
      df2.show()


      //8.
      val df3 = df1.filter(col("usage category")==="high usage")
        .agg(max("total_bill").alias("maximum bill amount"))
      df3.show()

      val df4 = df1.filter(col("usage category")==="medium usage")
        .agg(avg("kwh_usage").alias("average_kwh"))
      df4.show()

      //9.
      val df5 = df1.filter(col("usage category")==="low usage" && col("kwh_usage")>300)
        .agg(count("household").alias("count_household"))
      df5.show()

    }

}
