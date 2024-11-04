import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, initcap, count}

object Customer_Age_Grouping {
  def main(args:Array[String]):Unit=
    {
      val spark=SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val customers = List(
        ("karthik", 22),
        ("neha", 28),
        ("priya", 40),
        ("mohan", 55),
        ("ajay", 32),
        ("vijay", 18),
        ("veer", 47),
        ("aatish", 38),
        ("animesh", 60),
        ("nishad", 25)
      ).toDF("name", "age")

      val df1 = customers.withColumn("age group"
        ,when(col("age")<25,"Youth")
          .when(col("age")>25 && col("age")<45,"Adult")
          .otherwise("Senior"))


      val df2 = df1.groupBy(col("age group")).agg(count("name"))
      df2.show()

      val df3 = df1.withColumn("name",initcap(col("name")))
      df3.show()

    }

}
