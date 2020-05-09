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

  val dataPath = "src/resources/data"
  private def writeData(df: DataFrame, mode:String = "overwrite"): Unit ={
    df.write.format("parquet")
      .mode(mode)
      .save(dataPath)
  }

  private def schemaMatch(df1:DataFrame, df2:DataFrame): Boolean ={
    val x = df1.schema.fields.map(x => (x.name, x.dataType))
    val y = df2.schema.fields.map(x => (x.name, x.dataType))
    x.diff(y).isEmpty && y.diff(x).isEmpty
  }

  private def dataMatch(df1:DataFrame, df2:DataFrame): Boolean ={
    // TODO: this comparison is still dependent on order of columns within a single row in database,
    //  see how can that be mitigated...
    (df1.count() == df2.count())
      (df1.except(df2).isEmpty) &&
      (df2.except(df1).isEmpty)
  }

  describe("Schema Validation, when all records have one schema"){

    it("should return no bad records when number of columns, column name and type match"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
        = SchemaValidation.validate(dataPath)

      assert(result)
      assert(colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.isEmpty)
      assert(actualBadRecords.collect().isEmpty)
    }

    it("should return the missing column and all records when missing column in all records"){
      import spark.implicits._
      val df = Seq((1), (2)).toDF("emp_id")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
        = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(!colCountMatch)
      assert(missingFields.sameElements(Array(("emp_name", StringType))))
      assert(extraFields.isEmpty)
      assert(schemaMatch(actualBadRecords, df))
      assert(dataMatch(actualBadRecords, df))
    }

    it("should return the extra column and all records when extra column in all records"){
      import spark.implicits._
      val df = Seq((1, "Adam", 10), (2, "Eve", 12)).toDF("emp_id", "emp_name", "emp_age")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
        = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(!colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.sameElements(Array(("emp_age", IntegerType))))
      assert(schemaMatch(actualBadRecords, df))
      assert(dataMatch(actualBadRecords, df))
    }

    it("should return the missing column and all records when col name mismatch in all records, aka missing column"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name_1")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(colCountMatch)
      assert(missingFields.sameElements(Array(("emp_name", StringType))))
      assert(extraFields.sameElements(Array(("emp_name_1", StringType))))
      assert(schemaMatch(actualBadRecords, df))
      assert(dataMatch(actualBadRecords, df))
    }

    it("should return the missing column and all records when col type mismatch in all records, aka missing column"){
      import spark.implicits._
      val df = Seq((1.0, "Adam"), (2.1, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(colCountMatch)
      assert(missingFields.sameElements(Array(("emp_id", IntegerType))))
      assert(extraFields.sameElements(Array(("emp_id", DoubleType))))
      assert(schemaMatch(actualBadRecords, df))
      assert(dataMatch(actualBadRecords, df))
    }

    it("should return NO Bad records when nullable columns are null"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, null)).toDF("emp_id", "emp_name")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(result)
      assert(colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.isEmpty)
      assert(actualBadRecords.count() == 0)
    }

    it("should return Bad Records when NON-NULLABLE columns are null"){
      import spark.implicits._
      val df = Seq[(Integer, String)]((null, "Adam"), (2, null)).toDF("emp_id", "emp_name")
      writeData(df)

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.isEmpty)

      val expectedBadRecords =
        Seq[(Integer, String)]((null, "Adam"))
          .toDF("emp_id","emp_name")
      assert(schemaMatch(actualBadRecords, expectedBadRecords))
      assert(dataMatch(actualBadRecords, expectedBadRecords))
    }
  }

  describe("Schema Validation, when records have multiple schema"){

    it("should get bad records for missing fields for NON_NULLABLE columns in some rows") {
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)
      val df1 = Seq(("Caty"), ("Dan")).toDF("emp_name")
      writeData(df1, "append")

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.isEmpty)

      val expectedBadRecords =
        Seq[(Integer, String)]((null, "Caty"), (null, "Dan"))
          .toDF("emp_id","emp_name")

      assert(schemaMatch(actualBadRecords, expectedBadRecords))
      // Since the ordering of columns within a row was not fixed
      val expected = expectedBadRecords
        .select(expectedBadRecords.col("emp_id"), expectedBadRecords.col("emp_name"))
      val actual = actualBadRecords
        .select(actualBadRecords.col("emp_id"), actualBadRecords.col("emp_name"))
      assert(dataMatch(actual, expected))
    }

    it("should get NO bad records for missing fields for NULLABLE columns in some rows") {
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)
      val df1 = Seq((3), (4)).toDF("emp_id")
      writeData(df1, "append")

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(result)
      assert(colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields.isEmpty)
      assert(actualBadRecords.count() == 0)
    }

    it("should get bad records for extra fields in some rows") {
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Eve")).toDF("emp_id", "emp_name")
      writeData(df)
      val expectedBadRecords = Seq((1, "Adam", 10), (2, "Eve", 12))
        .toDF("emp_id", "emp_name", "emp_age")
      writeData(expectedBadRecords, "append")

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(!colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields sameElements Array(("emp_age", IntegerType)))

      assert(schemaMatch(actualBadRecords, expectedBadRecords))
      assert(dataMatch(actualBadRecords, expectedBadRecords))
    }

    it("should get bad records for extra fields and null values for NON_NULLABLE fields"){
      import spark.implicits._
      val df = Seq((1, "Adam"), (2, "Bob")).toDF("emp_id", "emp_name")
      writeData(df)

      val df1 = Seq((3, "Caty", 10), (4, "Dan", 12))
        .toDF("emp_id", "emp_name", "emp_age")
      writeData(df1, "append")

      val df2 = Seq[(Integer, String)]((null, "Eve"), (6, null))
        .toDF("emp_id", "emp_name")
      writeData(df2, "append")

      val (result, colCountMatch, missingFields, extraFields, actualBadRecords)
      = SchemaValidation.validate(dataPath)

      assert(!result)
      assert(!colCountMatch)
      assert(missingFields.isEmpty)
      assert(extraFields sameElements Array(("emp_age", IntegerType)))

      val expectedBadRecords = Seq[(Integer, String, Integer)](
        (3, "Caty", 10), (4, "Dan", 12), (null, "Eve", null))
        .toDF("emp_id", "emp_name", "emp_age")
      assert(schemaMatch(actualBadRecords, expectedBadRecords))
      assert(dataMatch(actualBadRecords, expectedBadRecords))
    }
  }
}
