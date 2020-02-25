package me.rotemfo

import me.rotemfo.UdfStore.udfUserAgent
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._

//noinspection ScalaCustomHdfsFormat,ScalaCustomHdfsFormat
object Mone extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    try {

      //      val moneDF = spark.read.json("/data/mone/*/*.gz")
      //        .filter(col("page_type")
      //          .eqNullSafe(lit("responsive")))
      //        .select(UdfStore.qtrim(col("user_agent")))
      //        .distinct()
      //        .union(userAgentsDF)
      //      FileUtils.deleteQuietly(new java.io.File(moneOutputDir))
      //      moneDF.coalesce(1).write.format(DATABRICKS_CSV).save(moneOutputDir)

      val moneOutputDir = s"$OUTPUT_DIR/mone_user_agents"
      val uaDF = spark.read.format(DATABRICKS_CSV)
        .option("header", value = false)
        .load(s"$moneOutputDir/*.csv")

      val dataOutputDir = s"$OUTPUT_DIR/mone_user_agents_data"
      FileUtils.deleteQuietly(new java.io.File(dataOutputDir))

      val col1 = uaDF.schema.fields(0).name
      uaDF.withColumnRenamed(col1, "user_agent")
        .filter(col("user_agent").isNotNull.and(
          lower(col("user_agent")).contains("seekingalpha")).or(col("user_agent").startsWith("sa"))
        )
        .withColumn("user_agent_json", udfUserAgent(col("user_agent")))
        .withColumn("_tmp", split(col("user_agent_json"), ","))
        .select(
          col("_tmp").getItem(0).as("os_name"),
          col("_tmp").getItem(1).as("os_version"),
          col("_tmp").getItem(2).as("agent_name"),
          col("_tmp").getItem(3).as("agent_version"),
          col("_tmp").getItem(4).as("device_class"),
          col("_tmp").getItem(5).as("device_version"),
          col("_tmp").getItem(6).as("device_brand"),
          col("user_agent"))
        .drop("_tmp")
        .select("user_agent", "os_name", "os_version", "agent_name", "agent_version", "device_class", "device_version", "device_brand")
        .coalesce(1)
        .write.format(DATABRICKS_CSV).option("header", value = true).save(dataOutputDir)
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      spark.close()
    }
  }
}
