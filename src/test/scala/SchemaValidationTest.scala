import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class SchemaValidationTest extends FunSuite{
  lazy val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("validate schema")
    .getOrCreate()

  val dataPath = "src/test/resources/data"

  private def writeData(df: DataFrame): Unit ={
    df.write.format("parquet")
      .mode("overwrite")
      .save(dataPath)
  }

  test("should return true when number of columns, column name and type match"){
    import spark.implicits._
    val df = Seq((1, "Adam"), (2, "Eve")).toDF("id", "name")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) == (true, Set(), Set()))
  }

  test("should return false when extra columns"){
    import spark.implicits._
    val df = Seq((1, "Adam", 10), (2, "Eve", 12)).toDF("id", "name", "age")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (false, Set(StructField("age",IntegerType,true)), Set()))
  }

  test("should return false when missing columns"){
    import spark.implicits._
    val df = Seq((1), (2)).toDF("id")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (false, Set(), Set(StructField("name",StringType,true))))
  }

  test("should return false when column name mismatch"){
    import spark.implicits._
    val df = Seq((1, "Adam"), (2, "Eve")).toDF("id", "name1")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (true,
        Set(StructField("name1",StringType,true)),
        Set(StructField("name",StringType,true))))
  }

  test("should return false when column type mismatch"){
    import spark.implicits._
    val df = Seq((1.0, "Adam"), (2.1, "Eve")).toDF("id", "name")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (true,
        Set(StructField("id",DoubleType,true)),
        Set(StructField("id",IntegerType,true))))
  }





}
