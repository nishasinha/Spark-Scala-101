import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class SchemaValidationTest extends FunSuite{
  lazy val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("validate schema")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val dataPath = "src/test/resources/data"
  private def writeData(df: DataFrame): Unit ={
    df.write.format("parquet")
      .mode("overwrite")
      .save(dataPath)
  }

  test("should return true when number of columns, column emp_name and type match"){
    import spark.implicits._
    val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) == (true, Set(), Set()))
  }

  test("should return false and extra columns when extra columns"){
    import spark.implicits._
    val df = Seq((1, "Adam", 10), (2, "Eve", 12)).toDF("emp_id", "emp_name", "emp_age")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (false, Set(StructField("emp_age",IntegerType,true)), Set()))
  }

  test("should return false and missing columns when missing columns"){
    import spark.implicits._
    val df = Seq((1), (2)).toDF("emp_id")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (false, Set(), Set(StructField("emp_name",StringType,true))))
  }

  test("should return true and extra and missing columns when column emp_name mismatch"){
    import spark.implicits._
    val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name_1")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (true,
        Set(StructField("emp_name_1",StringType,true)),
        Set(StructField("emp_name",StringType,true))))
  }

  test("should return true and mismatch types when column type mismatch"){
    import spark.implicits._
    val df = Seq((1.0, "Adam"), (2.1, "Eve")).toDF("emp_id", "emp_name")
    writeData(df)
    assert(SchemaValidation.validate(dataPath) ==
      (true,
        Set(StructField("emp_id",DoubleType,true)),
        Set(StructField("emp_id",IntegerType,true))))
  }





}
