import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
 * Find top 3 most popular hotels between couples.
 * (treat hotel as composite key of continent, country and market).
 * Implement using scala or python. Create a separate application.
 * Copy the application to the archive. Make screenshots of results: before and after execution.
 */
object Task1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDataFrameExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val testSchema = new StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("date_time", StringType, nullable = true),
        StructField("site_name", StringType, nullable = true),
        StructField("posa_continent", StringType, nullable = true),
        StructField("user_location_country", StringType, nullable = true),
        StructField("user_location_region", StringType, nullable = true),
        StructField("user_location_city", StringType, nullable = true),
        StructField("orig_destination_distance", StringType, nullable = true),
        StructField("user_id", StringType, nullable = true),
        StructField("is_mobile", StringType, nullable = true),
        StructField("is_booking", StringType, nullable = true),
        StructField("channel", StringType, nullable = true),
        StructField("srch_ci", StringType, nullable = true),
        StructField("srch_co", StringType, nullable = true),
        StructField("srch_adults_cnt", StringType, nullable = true),
        StructField("srch_children_cnt", StringType, nullable = true),
        StructField("srch_rm_cnt", StringType, nullable = true),
        StructField("srch_destination_id", StringType, nullable = true),
        StructField("srch_destination_type_id", StringType, nullable = true),
        StructField("hotel_continent", StringType, nullable = true),
        StructField("hotel_country", StringType, nullable = true),
        StructField("hotel_market", StringType, nullable = true)
      ))

    val df = spark.read.format("csv")
      .schema(testSchema)
      .load("D:\\Courses\\Big_Data\\1_HADOOP\\all\\test.csv")

    df.select(
      df.col("hotel_country")
    ).where("is_booking = 1")
      .groupBy("hotel_country")
      .count()
        .sort(desc("count"))
        .head(3)
        .foreach(row => println(row))

    //    terminate spark context
    spark.stop()
  }
}
