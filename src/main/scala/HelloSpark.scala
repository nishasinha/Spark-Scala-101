import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    println("Hello Spark!")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Hello Spark")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val data = (1 to 50).toDS()
    data
      .filter(_ % 10 == 0)
      .foreach(println(_))

    println("********* Running schema validation **************")
    val dataPath = "data-to-test"

    println("Given Schema")
    SchemaValidation.givenSchema.foreach(println)

    println("Input data read")
    val dataDF = spark.read.option("mergeSchema", "true").load(dataPath)
    dataDF.show

    println("Running validation")
    val result = SchemaValidation.validate(dataPath)

    println("Schema validation result:", result._1)
    println("Column count Match", result._2)
    println("Missing Columns")
    result._3.foreach(println)
    println("Extra Columns")
    result._4.foreach(println)
    println("Bad Records")
    result._5.show
  }
}
