import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    println("Hello Spark!")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("word count")
      .getOrCreate()

    import spark.implicits._

    val data = (1 to 50).toDS()
    data
      .filter(_ % 10 == 0)
      .foreach(println(_))
  }
}
