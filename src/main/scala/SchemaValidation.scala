import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaValidation {
  val givenSchema =
    StructType(
      Array(
        StructField("id", IntegerType),
        StructField("name", StringType)
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
    val missingCols = dataSchema.toSet.diff(givenSchema.toSet)
    val extraCols = givenSchema.toSet.diff(dataSchema.toSet)
    (columnCountMatch, missingCols, extraCols)
  }
}
