package utils

import scala.io.Source

object PersonData {
  def read(): Seq[Person] =
    Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("people.csv"))
      .getLines().map(line => {
      val parts: Array[String] = line.split(",")
      Person(parts(0), parts(1), parts(2).toInt, parts(3))
    }).toSeq

}