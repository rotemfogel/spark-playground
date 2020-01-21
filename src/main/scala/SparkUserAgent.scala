import java.io.File

import UdfStore._
import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * project: spark-demo
 * package: 
 * file:    SparkUserAgent
 * created: 2019-10-17
 * author:  rotem
 */
//noinspection ScalaCustomHdfsFormat
object SparkUserAgent extends BaseSparkApp with Logging {

  private final val schema: StructType = StructType(
    Array(
      StructField("user_agent", StringType, nullable = false),
      StructField("user_id", StringType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()

    val path = "/home/rotem/Documents/sa/vertica/user_agents/user_agents_full.csv"
    val userAgentDf = spark.read
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = false)
      .schema(schema)
      .csv(path)

    val outputDir = OUTPUT_DIR + "/user_agents"
    FileUtils.deleteQuietly(new File(outputDir))

    userAgentDf
      .filter(col("user_agent").contains("seekingalpha"))
      .withColumn("user_agent_json", udfUserAgent(col("user_agent")))
      .withColumn("_tmp", split(col("user_agent_json"), ","))
      .select(
        col("_tmp").getItem(0).as("osName"),
        col("_tmp").getItem(1).as("browserName"),
        col("_tmp").getItem(2).as("version"),
        col("user_id"))
      .drop("_tmp").drop("user_agent_json")
      // .where(col("osName").isin("Mac OS", "macOS", "Mac OS X"))
      .select("osName", "browserName", "version", "user_id")
      .coalesce(1)
      .write.format("com.databricks.spark.csv").option("header", value = true).save(outputDir)

    spark.stop()
  }
}
