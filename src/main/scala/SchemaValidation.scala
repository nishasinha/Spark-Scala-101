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

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("validate schema")
    .getOrCreate()

  def validate(dataPath: String):
  (Boolean, Array[(String, DataType)], Array[(String, DataType)],sql.DataFrame) = {
    var result = true
    val dataDF = spark.read.option("mergeSchema", "true").load(dataPath)
    val dataSchema = dataDF.schema

    val columnCountMatch = givenSchema.fields.length == dataSchema.fields.length
    val (missingCols, extraCols) = getMissingAndExtraColumnsFromData(dataSchema)

    if(missingCols.length > 0){
      result = false
      return (columnCountMatch, missingCols, extraCols, dataDF)
    }

    if(extraCols.length > 0){
      result = false
      val extraColName = extraCols(0)._1
      val extraFieldBadRecords = dataDF.filter(dataDF.col(extraColName).isNotNull).toDF()
      return (columnCountMatch, missingCols, extraCols, extraFieldBadRecords)
    }

    // column count match here
    val nullViolatingRecords = dataDF.filter(dataDF.col("emp_id").isNull).toDF()
    result = nullViolatingRecords.count() == 0
    (columnCountMatch, missingCols, extraCols, nullViolatingRecords)
  }

  private def getMissingAndExtraColumnsFromData(dataSchema: StructType) = {
    val dataSchemaWithoutNulls = dataSchema.fields.map(x => (x.name, x.dataType))
    val givenSchemaWithoutNulls = givenSchema.fields.map(x => (x.name, x.dataType))

    val missingCols = givenSchemaWithoutNulls.diff(dataSchemaWithoutNulls)
    val extraCols = dataSchemaWithoutNulls.diff(givenSchemaWithoutNulls)
    (missingCols, extraCols)
  }
}
