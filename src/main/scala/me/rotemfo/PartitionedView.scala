package me.rotemfo

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

import java.io.File

object PartitionedView extends BaseSparkApp {
  private val schema = StructType(
    Seq(
      StructField("req_time", StringType, nullable = true),
      StructField("machine_ip", StringType, nullable = true),
      StructField("px_score", StringType, nullable = true),
      StructField("user_agent", StringType, nullable = true),
      StructField("version", StringType, nullable = true),
      StructField("page_type", StringType, nullable = true),
      StructField("page_key", StringType, nullable = true),
      StructField("referrer_key", StringType, nullable = true),
      StructField("referrer", StringType, nullable = true),
      StructField("event_name", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("url_params", StringType, nullable = true),
      StructField("machine_cookie", StringType, nullable = true),
      StructField("session_cookie", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("user_email", StringType, nullable = true),
      StructField("user_vocation", StringType, nullable = true),
      StructField("other", StringType, nullable = true)
    )
  )

  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File("spark-warehouse"))
    val spark = getSparkSession()
    val df1 = spark.read.schema(schema).json("/data/events/stag/mone_posts/2020/12/27/08/*.gz")
      .withColumn("date_", lit("2020-12-27"))
      .withColumn("hour", lit("08"))
    val df2 = spark.read.schema(schema).json("/data/events/stag/mone_posts/2020/12/27/09/*.gz")
      .withColumn("date_", lit("2020-12-27"))
      .withColumn("hour", lit("09"))
    df1.union(df2)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("date_", "hour")
      .saveAsTable("mone_posts")

    val sqlText = "SELECT COUNT(1) FROM mone_posts WHERE hour='08'"
    val plan = spark.sessionState.sqlParser.parsePlan(sqlText)
    log.info(plan.numberedTreeString)
    spark.sqlContext.sql(sqlText).show()

    val df = spark.read.parquet("spark-warehouse/mone_posts/date_=2020-12-07/hour=08/*.parquet", "spark-warehouse/mone_posts/date_=2020-12-07/hour=09/*.parquet")
    df.printSchema()

  }
}
