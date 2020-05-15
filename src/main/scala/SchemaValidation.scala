import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
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

  def validate(dataPath: String)
  :(Boolean, Boolean, Array[(String, DataType)], Array[(String, DataType)],sql.DataFrame) = {
    val dataDF = spark.read.option("mergeSchema", "true").load(dataPath)
    val dataSchema = dataDF.schema

    val columnCountMatch = givenSchema.fields.length == dataSchema.fields.length
    val (missingCols, extraCols) = getMissingAndExtraColumnsFromData(dataSchema)

    if(missingCols.length > 0){
      return (false, columnCountMatch, missingCols, extraCols, dataDF)
    }

    var badRecordsDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataSchema)
    badRecordsDF = badRecordsDF.union(getRecordsWithExtraCols(dataDF, extraCols))
    badRecordsDF = badRecordsDF.union(getRecordsWithNullValuesForNonNullableCols(dataDF))

    (badRecordsDF.count() == 0, columnCountMatch, missingCols, extraCols, badRecordsDF)
  }

  private def getMissingAndExtraColumnsFromData(dataSchema: StructType) = {
    val dataSchemaWithoutNulls = dataSchema.fields.map(x => (x.name, x.dataType))
    val givenSchemaWithoutNulls = givenSchema.fields.map(x => (x.name, x.dataType))

    val missingCols = givenSchemaWithoutNulls.diff(dataSchemaWithoutNulls)
    val extraCols = dataSchemaWithoutNulls.diff(givenSchemaWithoutNulls)
    (missingCols, extraCols)
  }

  private def getRecordsWithExtraCols(dataDF:sql.DataFrame,
                                    extraCols: Array[(String, DataType)])
  :sql.DataFrame = {
    var badRecords = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataDF.schema)
    if (extraCols.length > 0) {
      val conditions = extraCols.map(x => dataDF.col(x._1).isNotNull)
      for (condition <- conditions) {
        val extraColDF = dataDF.filter(condition)
        if (!extraColDF.isEmpty) {
          badRecords = badRecords.union(extraColDF)
        }
      }
    }
    badRecords
  }

  private def getRecordsWithNullValuesForNonNullableCols(dataDF:sql.DataFrame)
  :sql.DataFrame = {
    var badRecords = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataDF.schema)
    val nonNullableCols = givenSchema.fields.filter(x=> (!x.nullable))
    if(nonNullableCols.length > 0){
      val conditions = nonNullableCols.map(x=> dataDF.col(x.name).isNull)
      for(condition <- conditions){
        val badDF = dataDF.filter(condition)
        if(!badDF.isEmpty){
          badRecords = badRecords.union(badDF)
        }
      }
    }
    badRecords
  }
}
