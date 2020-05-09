import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

object SchemaValidation {
  val givenSchema =
    StructType(
      Array(
        StructField("emp_id", IntegerType,false),
        StructField("emp_name", StringType, true)
      )
    )

  val badRecordsPath = "src/test/resources/badRecords"

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("validate schema")
    .getOrCreate()

  def validate(dataPath: String):
  (Boolean, Boolean, Array[(String, DataType)], Array[(String, DataType)],sql.DataFrame) = {
    var result = true
    FileUtils.deleteDirectory(new File(badRecordsPath))
    val dataDF = spark.read.option("mergeSchema", "true").load(dataPath)
    val dataSchema = dataDF.schema

    val columnCountMatch = givenSchema.fields.length == dataSchema.fields.length
    val (missingCols, extraCols) = getMissingAndExtraColumnsFromData(dataSchema)

    if(missingCols.length > 0){
      result = false
      return (result, columnCountMatch, missingCols, extraCols, dataDF)
    }

    if(extraCols.length > 0){
      result = false
      val extraColName = extraCols(0)._1
      val extraFieldBadRecords = dataDF.filter(dataDF.col(extraColName).isNotNull).toDF()
      extraFieldBadRecords.show()
      writeBadRecords(extraFieldBadRecords)
    }

    // column count match here
    val nullViolatingRecords = dataDF.filter(dataDF.col("emp_id").isNull).toDF()
    writeBadRecords(nullViolatingRecords)

    result = result && nullViolatingRecords.count() == 0
    val badRecords = spark.read.option("mergeSchema", "true").load(badRecordsPath)
    (result, columnCountMatch, missingCols, extraCols, badRecords)
  }

  private def getMissingAndExtraColumnsFromData(dataSchema: StructType) = {
    val dataSchemaWithoutNulls = dataSchema.fields.map(x => (x.name, x.dataType))
    val givenSchemaWithoutNulls = givenSchema.fields.map(x => (x.name, x.dataType))

    val missingCols = givenSchemaWithoutNulls.diff(dataSchemaWithoutNulls)
    val extraCols = dataSchemaWithoutNulls.diff(givenSchemaWithoutNulls)
    (missingCols, extraCols)
  }

  private def writeBadRecords(df: sql.DataFrame): Unit ={
    df
      .write.format("parquet")
      .mode("append")
      .save(badRecordsPath)
  }
}
