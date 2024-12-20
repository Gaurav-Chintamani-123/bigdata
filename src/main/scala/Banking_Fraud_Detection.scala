import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count, avg, min, sum, max}

object Banking_Fraud_Detection {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val transactions = List(
        ("Account1", "2024-11-01", 12000, 6, "Savings"),
        ("Account2", "2024-11-01", 8000, 3, "Current"),
        ("Account3", "2024-11-02", 2000, 1, "Savings"),
        ("Account4", "2024-11-02", 15000, 7, "Savings"),
        ("Account5", "2024-11-03", 9000, 4, "Current"),
        ("Account6", "2024-11-03", 3000, 1, "Current"),
        ("Account7", "2024-11-04", 13000, 5, "Savings"),
        ("Account8", "2024-11-04", 6000, 2, "Current"),
        ("Account9", "2024-11-05", 20000, 8, "Savings"),
        ("Account10", "2024-11-05", 7000, 3, "Savings")
      ).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")

      val df1 = transactions.withColumn("risk level"
      ,when(col("amount")>1000 && col("frequency")>5,"high risk")
          .when(col("amount").between(5000,10000) && col("frequency").between(2,5),"moderate risk")
          .otherwise("low risk")
      )
      df1.show()

      //1
      val df2 = df1.groupBy("risk level")
        .agg(count("account_id").alias("count"))
      df2.show()

      //2
      val df3 = df1.filter(col("risk level")==="high risk")
        .agg(sum("amount").alias("total amount"))
      df3.show()

      //3
      val df4 = df1.filter(col("risk level")==="moderate level" &&
      col("account_type")==="Savings" &&
      col("amount")>7500)
      df4.show()

    }

}
