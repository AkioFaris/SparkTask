import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

class Task1Test extends FlatSpec {

  "Test ReservationsTableTransformation" should "most popular countries where booking is successful" in {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Task1")
      .getOrCreate()

    val tablePath = Paths.get(getClass.getResource("testShort.csv").toURI).toString
    val table = TableLoader.loadTestTable(sparkSession, tablePath)
    val result = ReservationsTableTransformation.mostPopularCountriesWithSuccessfulBooking(table)
    val expected = sparkSession.read.format("csv")
      .option("header", "true")
      .schema(StructType(Array(
        StructField("hotel_country", StringType, true),
        StructField("count", IntegerType, true)
      )))
      .load("src\\test\\resources\\expectedResult.csv")

    assert(expected.toJSON.collect().sameElements(result.toJSON.collect()))
    sparkSession.stop()
  }
}