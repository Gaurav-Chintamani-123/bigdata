import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, avg}


object Customer_Spending_Pattern {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val customers = List(
        ("karthik", "Premium", 1050, 32),
        ("neha", "Standard", 800, 28),
        ("priya", "Premium", 1200, 40),
        ("mohan", "Basic", 300, 35),
        ("ajay", "Standard", 700, 25),
        ("vijay", "Premium", 500, 45),
        ("veer", "Basic", 450, 33),
        ("aatish", "Standard", 600, 29),
        ("animesh", "Premium", 1500, 60),
        ("nishad", "Basic", 200, 21)
      ).toDF("name", "membership", "spending", "age")

      val df1 = customers.withColumn("spending category"
      ,when((col("spending")>1000) && (col("membership") === "Premium"),"High Spender")
          .when((col("spending").between(500,1000)) && (col("membership")==="Standard"),"Average Spender")
          .otherwise("Low Spender")
      )
      df1.show()

      val df2 = df1.groupBy("membership").agg(avg("spending")).alias("avg spending")
      df2.show()

    }

}
