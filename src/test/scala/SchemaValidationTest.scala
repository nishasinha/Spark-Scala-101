import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSpec

class SchemaValidationTest extends FunSpec{
  lazy val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("validate schema")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val dataPath = "src/it/resources/data"
  private def writeData(df: DataFrame): Unit ={
    df.write.format("parquet")
      .mode("overwrite")
      .save(dataPath)
  }

  describe("Schema Validation"){
    it("should return true when number of columns, column emp_name and type match"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) == (true, Set(), Set()))
    }

    it("should return false and missing columns when missing columns"){
      import spark.implicits._
      val df = Seq((1), (2)).toDF("emp_id")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) ==
        (false, Set(StructField("emp_name",StringType,true)), Set()))
    }

    it("should return false and extra columns when extra columns"){
      import spark.implicits._
      val df = Seq((1, "Adam", 10), (2, "Eve", 12)).toDF("emp_id", "emp_name", "emp_age")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) ==
        (false, Set(), Set(StructField("emp_age",IntegerType,true))))
    }

    it("should return true and missing, extra columns when column emp_name mismatch"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name_1")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) ==
        (true,
          Set(StructField("emp_name",StringType,true)),
          Set(StructField("emp_name_1",StringType,true))))
    }

    it("should return true and mismatch types when column type mismatch"){
      import spark.implicits._
      val df = Seq((1.0, "Adam"), (2.1, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) ==
        (true,
          Set(StructField("emp_id",IntegerType,true)),
          Set(StructField("emp_id",DoubleType,true))))
    }

    it("should return true and no mismatch when null criteria match"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, null)).toDF("emp_id", "emp_name")
      writeData(df)
      assert(SchemaValidation.validate(dataPath) ==  (true, Set(), Set()))
    }
  }

  describe("Get Bad Records"){
    it("should return true and bad records when null criteria mismatch"){
      import spark.implicits._
      val df = Seq[(Integer, String)]((null, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)

      assert(SchemaValidation.validate(dataPath) ==  (true, Set(), Set()))

      val expectedBadRecordsDF = Seq[(Integer, String)]((null, "Adam")).toDF("emp_id", "emp_name")
      val actualBadRecords = SchemaValidation.getBadRecords(dataPath)
      assert(actualBadRecords.schema.fields sameElements(expectedBadRecordsDF.schema.fields))
      assert(actualBadRecords.collect() sameElements(expectedBadRecordsDF.collect()))
    }
  }
}
