import org.apache.spark.sql.SparkSession

/**
 * Find top 3 most popular hotels between couples.
 * (treat hotel as composite key of continent, country and market).
 */
object Task1 {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Task1")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val df = TableLoader.loadTestTable(sparkSession, args(0))
    ReservationsTableTransformation.top3MostPopularHotelsBetweenCouples(df)
      .head(3)
      .foreach(row => println(row))

    sparkSession.stop()
  }
}
