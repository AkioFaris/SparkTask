import org.apache.spark.sql.SparkSession

object Task1 {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Task1")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val df = TableLoader.loadTestTable(sparkSession, args(0))
    ReservationsTableTransformation.mostPopularCountriesWithSuccessfulBooking(df)
      .head(3)
      .foreach(row => println(row))

    sparkSession.stop()
  }
}
