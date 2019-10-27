import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object ReservationsTableTransformation {

  /**
   * Finds most popular countries where booking is successful
   */
  def mostPopularCountriesWithSuccessfulBooking(df: DataFrame): DataFrame = df.select(
    df.col("hotel_country")
  ).where("is_booking = 1")
    .groupBy("hotel_country")
    .count()
    .sort(desc("count"))
}
