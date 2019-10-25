import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{datediff, _}
import org.apache.spark.sql.types._

object Task2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDataFrameExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val testSchema = new StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("date_time", DateType, nullable = true),
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
        StructField("srch_ci", DateType, nullable = true),
        StructField("srch_co", DateType, nullable = true),
        StructField("srch_adults_cnt", IntegerType, nullable = true),
        StructField("srch_children_cnt", IntegerType, nullable = true),
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
      datediff(df.col("srch_co"), df.col("srch_ci")).as("longest_period")
    ).where("srch_adults_cnt = 2")
      .where("srch_children_cnt > 0")
      .sort(desc("longest_period"))
      .head(1)
      .foreach(row => println(row))

    //    terminate spark context
    spark.stop()
  }
}
