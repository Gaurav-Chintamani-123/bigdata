import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, initcap, count}

object Vehicle_Mileage_Analysis {
  def main(args:Array[String]):Unit=
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val vehicles = List(
        ("CarA", 30),
        ("CarB", 22),
        ("CarC", 18),
        ("CarD", 15),
        ("CarE", 10),
        ("CarF", 28),
        ("CarG", 12),
        ("CarH", 35),
        ("CarI", 25),
        ("CarJ", 16)
      ).toDF("vehicle_name", "mileage")

      val df1 = vehicles.select(col("vehicle_name"),col("mileage")
      ,when(col("mileage")>25,"High Efficiency")
          .when(col("mileage")>=15 && col("mileage")<=25,"Moderate Efficiency")
          .otherwise(("Low Efficinecy"))
          .alias("Efficinecy level")
      )

      df1.show()

    }
}
