import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ScalaDataFrameExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDataFrameExample")
      .getOrCreate()

    val peopleSchema = new StructType(
      Array(
        StructField("first_name", StringType, nullable = true),
        StructField("last_name", StringType, nullable = true),
        StructField("age", StringType, nullable = true),
        StructField("state", StringType, nullable = true)
      ))

    val df = spark.read.format("csv")
      .schema(peopleSchema)
      .load("D:\\Courses\\Big_Data\\5_SPARK\\SparkTask\\src\\main\\resources\\people.csv")
    // verbose syntax
    println("Under 25")
    df.select(
      df.col("first_name").as("Name"),
      col("last_name").as("Surname"),
      expr("age as Years").cast("int"),
      lit(1).as("something")
    ).where("Years < 25")
      .orderBy("Years")
      .collect
      .foreach(row => println(row))

    df.select(
      df.col("first_name").as("Name"),
      col("last_name").as("Surname"),
      expr("age as Years").cast("int"),
      lit(1).as("something")
    ).where("Years > 25")
      .orderBy("Years")
      .collect
      .foreach(row => println(row))
    //    terminate spark context
    spark.stop()
  }
}
