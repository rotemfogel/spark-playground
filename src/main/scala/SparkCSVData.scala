import java.io.File

import UdfStore._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * project: spark-demo
 * package: 
 * file:    SparkReferrerCategory
 * created: 2019-10-17
 * author:  rotem
 */
object SparkCSVData extends BaseSparkApp {

  private final val schema = StructType(
    List(
      StructField("user_id", LongType, nullable = true),
      StructField("email", StringType, nullable = false),
      StructField("registration_date", DateType, nullable = false),
      StructField("registration_hour", IntegerType, nullable = false),
      StructField("confirmation_date", DateType, nullable = true),
      StructField("is_confirmed", IntegerType, nullable = false),
      StructField("user_agent", StringType, nullable = false),
      StructField("is_gplus", LongType, nullable = false),
      StructField("page_type", StringType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val _path = SparkCSVData.getClass.getResource("/").getPath
    val path = (_path.split(File.separator).dropRight(3) ++ Array("src", "main", "resources")).mkString(File.separator)

    val spark = getSparkSession(cores = 16, memory = 12)

    val data = spark.read
      .schema(schema)
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = false)
      .option("delimiter", ",")
      .csv(path + File.separator + "registrations-confirmations.csv")

    val userAgentDf = data
      .withColumn("user_agent_json", udfUserAgent(col("user_agent")))
      .withColumn("_tmp", split(col("user_agent_json"), ","))
      .select(
        col("user_id"),
        col("email"),
        col("registration_date"),
        col("registration_hour"),
        col("confirmation_date"),
        col("is_confirmed"),
        col("is_gplus"),
        col("page_type"),
        col("_tmp").getItem(0).as("os_name"),
        col("_tmp").getItem(1).as("browser_name"),
        col("_tmp").getItem(2).as("device_class"),
        col("_tmp").getItem(3).as("agent_class")
      )
      // .filter(not(col("agent_class").eqNullSafe("Mobile App")))
      .orderBy(col("registration_date"), col("registration_hour"), col("email"))

    val outputDir = OUTPUT_DIR + "/registration"
    FileUtils.deleteQuietly(new File(outputDir))

    userAgentDf
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", value = true).save(outputDir)

    spark.stop()
  }
}
