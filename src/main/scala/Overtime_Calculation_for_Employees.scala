import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, initcap}

object Overtime_Calculation_for_Employees {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val employees = List(
        ("karthik", 62),
        ("neha", 50),
        ("priya", 30),
        ("mohan", 65),
        ("ajay", 40),
        ("vijay", 47),
        ("veer", 55),
        ("aatish", 30),
        ("animesh", 75),
        ("nishad", 60)
      ).toDF("name", "hours_worked")

      val df1 = employees.select(col("name"),col("hours_worked")
        ,when(col("hours_worked")>60,"Excessive Overtime")
          .when(col("hours_worked")>45 && col("hours_worked")<60,"Standard Overtimr")
          .otherwise("No Overtime")
          .alias("Overtime Status"))
      df1.show()


      val df2 = df1.groupBy("Overtime Status")


      val df3 = df1.withColumn("name",initcap(col("name")))
        df3.show()

    }

}
