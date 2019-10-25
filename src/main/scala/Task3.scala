import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Task3 {
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
      df.col("hotel_continent"),
      df.col("hotel_country"),
      df.col("hotel_market")
    ).where("is_booking = 0")
      .groupBy("hotel_country", "hotel_country", "hotel_market")
      .count()
        .sort(desc("count"))
        .head(3)
        .foreach(row => println(row))

    spark.stop()
  }
}
