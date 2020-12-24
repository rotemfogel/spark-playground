package me.rotemfo

import me.rotemfo.UdfStore.qtrim
import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.File

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
      StructField("user_agent", StringType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()

    val path = "/home/rotem/Documents/sa/vertica/user_agents/user_agents_full.csv"
    val userAgentDf = spark.read
      .format(DATABRICKS_CSV)
      .option("header", value = true)
      .option("inferSchema", value = false)
      .schema(schema)
      .csv(path)

    val outputDir = OUTPUT_DIR + "/user_agents"
    FileUtils.deleteQuietly(new File(outputDir))

    val ua = getUserAgentAnalyzer(spark)
    val udfUserAgent = getUdfUserAgent(ua)

    userAgentDf
      .withColumn("user_agent_fixed", qtrim(col("user_agent")))
      //      .filter(col("user_agent_fixed").isNotNull.and(lower(col("user_agent_fixed")).contains("seekingalpha")))
      .filter(col("user_agent_fixed").isNotNull.and(col("user_agent_fixed").contains("SeekingAlpha")))
      .withColumn("user_agent_json", udfUserAgent(col("user_agent_fixed")))
      .withColumn("_tmp", split(col("user_agent_json"), ","))
      .select(
        col("_tmp").getItem(0).as("os_name"),
        col("_tmp").getItem(1).as("os_version"),
        col("_tmp").getItem(2).as("agent_name"),
        col("_tmp").getItem(3).as("agent_version"),
        col("_tmp").getItem(4).as("device_class"),
        col("_tmp").getItem(5).as("device_version"),
        col("_tmp").getItem(6).as("device_brand"))
      .drop("_tmp").drop("user_agent_json")
      // .where(col("osName").isin("Mac OS", "macOS", "Mac OS X"))
      .select("os_name", "os_version", "agent_name", "agent_version")
      .coalesce(1)
      .write.format(DATABRICKS_CSV).option("header", value = true).save(outputDir)

    spark.stop()
  }
}
