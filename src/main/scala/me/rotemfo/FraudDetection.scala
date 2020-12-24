package me.rotemfo

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//noinspection ScalaCustomHdfsFormat
object FraudDetection extends BaseSparkApp {

  private final lazy val schema = StructType(Array(
    StructField("ts", TimestampType, nullable = false),
    StructField("event_name", StringType, nullable = false),
    StructField("machine_cookie", StringType, nullable = true),
    StructField("session_cookie", StringType, nullable = true),
    StructField("page_key", StringType, nullable = true),
    StructField("user_id", IntegerType, nullable = true),
    StructField("user_agent", StringType, nullable = true),
    StructField("referrer", StringType, nullable = true),
    StructField("referrer_domain", StringType, nullable = true),
    StructField("url", StringType, nullable = true),
    StructField("content_id", IntegerType, nullable = true),
    StructField("machine_ip", StringType, nullable = true),
    StructField("client_type", StringType, nullable = true),
    StructField("country_geo", StringType, nullable = true),
    StructField("territory_geo", StringType, nullable = true)
  ))

  // outlierDetection
  private val detectOutlier = (value: Column, upperLimit: Column) => {
    value > upperLimit
  }

  private final lazy val articles = Map(
    4310293 -> "Bitcoin Corrects And What's Next",
    4310948 -> "Sub-$8,000 Bitcoin Is A Discount",
    4313218 -> "Bitcoin's Correction May Soon End")

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession("FraudDetection")
    try {
      import spark.implicits._
      val articlesDf = articles.toSeq.toDF("content_id", "title")

      val ua = getUserAgentAnalyzer(spark)
      val udfUserAgent = getUdfUserAgent(ua)

      val df = spark.read.format(DATABRICKS_CSV)
        .option("delimiter", ",")
        .option("header", value = true)
        .schema(schema)
        .load("/data/vertica/fraud/m-zack-norman.csv")
        .withColumn("date_", date_format(col("ts"), "yyyy-MM-dd"))
        .withColumn("hour_", date_format(col("ts"), "yyyy-MM-dd HH"))
        .withColumn("minute_", date_format(col("ts"), "mm"))
        .withColumn("user_agent_json", udfUserAgent(col("user_agent")))
        .withColumn("_tmp", split(col("user_agent_json"), ","))
        .withColumn("_tmp", split(col("user_agent_json"), ","))
        .withColumn("referrer_", when(col("referrer_domain").isNull, col("url").substr(0, 16)).otherwise(col("referrer_domain")))
        .select(
          col("_tmp").getItem(0).as("os_name"),
          col("_tmp").getItem(1).as("browser_name"),
          col("_tmp").getItem(2).as("version"),
          col("user_id"),
          col("date_"),
          col("content_id"),
          col("referrer_"),
          col("referrer"),
          col("referrer_domain"),
          col("country_geo"))
        .drop("user_agent_json")
        .join(articlesDf, Seq("content_id"))
        .cache()

      //      def groupings(col: String): Unit = {
      //        Array(4310948, 4313218).foreach(a => {
      //          df.select("content_id", col)
      //            .where(s"content_id = $a")
      //            .groupBy("content_id", col)
      //            .count()
      //            .orderBy(desc("count"), asc("content_id"))
      //            .show(10)
      //        })
      //      }

      //      groupings("referrer_")
      //      groupings("os_name")
      //      groupings("browser_name")
      //      groupings("country_geo")

      val pageViewsDf = df.select("date_", "content_id")
        .groupBy("date_", "content_id")
        .count()
        .withColumnRenamed("count", "page_views")

      val statsDf = pageViewsDf
        .groupBy("content_id")
        .agg(
          // mean("page_views").as("mean"),
          stddev("page_views").as("stddev")
        )
        .withColumn("upper_limit", col("stddev") * 3)
        .join(pageViewsDf, usingColumn = "content_id")

      val outlierDF = statsDf
        .select("content_id", "page_views", /*"mean", */ "stddev", "upper_limit")
        .withColumn("is_outlier", detectOutlier(col("page_views"), col("upper_limit")))
        .filter(col("is_outlier"))
        .join(pageViewsDf, usingColumns = Seq("content_id", "page_views"))

      outlierDF.show()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
