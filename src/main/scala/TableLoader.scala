import java.nio.file.{Files, Paths}

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TableLoader {

  def checkFileExists(fullPath: String): Unit = {
    if (!Files.isRegularFile(Paths.get(fullPath)))
      throw new RuntimeException("file " + fullPath + " does not exist.")
  }

  def loadTestTable(sparkSession: SparkSession, fullPath: String): DataFrame = {
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
    checkFileExists(fullPath)
    sparkSession.read.schema(testSchema).csv(fullPath)
  }

}
