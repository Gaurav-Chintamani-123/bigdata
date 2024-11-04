import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Project_Allocation_and_Workload_Analysis {
  def main(args:Array[String]):Unit=
    {

      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val workload = List(
        ("karthik", "ProjectA", 120),
        ("karthik", "ProjectB", 100),
        ("neha", "ProjectC", 80),
        ("neha", "ProjectD", 30),
        ("priya", "ProjectE", 110),
        ("mohan", "ProjectF", 40),
        ("ajay", "ProjectG", 70),
        ("vijay", "ProjectH", 150),
        ("veer", "ProjectI", 190),
        ("aatish", "ProjectJ", 60),
        ("animesh", "ProjectK", 95),
        ("nishad", "ProjectL", 210),
        ("varun", "ProjectM", 50),
        ("aadil", "ProjectN", 90)
      ).toDF("name", "project", "hours")

      val df1 = workload.withColumn("workload level"
        ,when(col("hours")>200,"Overloaded")
          .when(col("hours")>100 && col("hours")<200,"Balanced")
          .otherwise("Underutilized")
      )

      df1.show()


    }

}
