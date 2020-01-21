import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._

object Sorter extends BaseSparkApp {
  private final val PATH = "/data/vertica/ido/"
  private final val FILES = Array("training_201906.csv.bz2", "training_201907.csv.bz2", "test_201908.csv.bz2", "test_201909.csv.bz2")
  private final val OUTPUT = "/data/vertica/result/"
  private final val CODEC = "org.apache.hadoop.io.compress.BZip2Codec"

  private val schema = StructType(
    Seq(
      StructField("ts", TimestampType, nullable = false),
      StructField("user_id", IntegerType, nullable = true),
      StructField("page_type", StringType, nullable = true),
      StructField("first_page_event", TimestampType, nullable = false),
      StructField("last_page_event", TimestampType, nullable = false),
      StructField("page_key", StringType, nullable = false),
      StructField("block_start", IntegerType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("Sorter")
    FILES.foreach(file => {
      val input = PATH + file
      val df = spark.read
        .format("com.databricks.spark.csv")
        .option("header", value = true)
        .option("inferSchema", value = false)
        .option("codec", CODEC)
        .schema(schema)
        .csv(input)

      import org.apache.spark.sql.functions.col

      val f = file.split("\\.")(0)
      val outputDir = OUTPUT + f
      FileUtils.deleteQuietly(new File(outputDir))

      df.filter(col("user_id").isNotNull)
        .repartition(col("user_id"))
        .sort(col("user_id"), col("ts"))
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", value = true)
        .option("codec", CODEC)
        .save(outputDir)
    })
    spark.stop()
  }

}
