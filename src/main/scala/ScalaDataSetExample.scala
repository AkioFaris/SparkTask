import utils.PersonData
import org.apache.spark.sql.SparkSession

object ScalaDataSetExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkDataSetExample")
      .getOrCreate() // load initial RDD
    import spark.implicits._
    val dataset = spark.createDataset(PersonData.read())
    // verbose syntax
    println("Under 21")
    dataset.filter(p => p.age < 21)
      .collect.foreach(println)
//    concise syntax
      println("Over 21")
    dataset.filter(_.age > 21)
      .collect
      .foreach(println)
//    terminate spark context
    spark.stop()
  }
}
