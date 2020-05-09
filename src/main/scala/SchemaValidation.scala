import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

  def validate(dataPath: String): (Boolean, Set[StructField], Set[StructField]) = {
    val dataDF = spark.read.format("parquet").load(dataPath)
    val dataSchema = dataDF.schema

    val columnCountMatch = givenSchema.fields.length == dataSchema.fields.length
    val (missingCols, extraCols) = getMissingAndExtraColumnsFromData(dataSchema)

    (columnCountMatch, missingCols, extraCols)
  }

  private def getMissingAndExtraColumnsFromData(dataSchema: StructType) = {
    val dataSchemaWithoutNulls = dataSchema.map(_.copy(nullable = true)).toSet
    val givenSchemaWithoutNulls = givenSchema.map(_.copy(nullable = true)).toSet
    val missingCols = givenSchemaWithoutNulls.diff(dataSchemaWithoutNulls)
    val extraCols = dataSchemaWithoutNulls.diff(givenSchemaWithoutNulls)
    (missingCols, extraCols)
  }

  def getBadRecords(dataPath:String)={
    val dataDF = spark.read.format("parquet").load(dataPath)
    val nullViolatingRecords = dataDF.filter(dataDF.col("emp_id").isNull)
    nullViolatingRecords.toDF()
  }


}
