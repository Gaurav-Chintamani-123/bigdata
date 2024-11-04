import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, datediff, current_date, initcap}

object emp_status_check {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._


      val employees = List(
        ("karthik", "2024-11-01"),
        ("neha", "2024-10-20"),
        ("priya", "2024-10-28"),
        ("mohan", "2024-11-02"),
        ("ajay", "2024-09-15"),
        ("vijay", "2024-10-30"),
        ("veer", "2024-10-25"),
        ("aatish", "2024-10-10"),
        ("animesh", "2024-10-15"),
        ("nishad", "2024-11-01"),
        ("varun", "2024-10-05"),
        ("aadil", "2024-09-30")).toDF("name", "last_checkin")



      val df =  current_date()

      val df2 = employees.withColumn("status",
        when(datediff(df, col("last_checkin")) <= 7, "Active")
          .otherwise("Inactive"))
        .withColumn("name",initcap(col("name")))

      df2.show()

    }

}
