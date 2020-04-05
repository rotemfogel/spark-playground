package me.rotemfo

import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode

object DisclosureTemplates extends BaseSparkApp with Logging {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession(params = Map("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> 2))

    val path = "/data/mariadb/disclosure_key_values/*"
    val df = spark.read.parquet(path)

    df.where(col("instablog_post_id").isin(2771893, 3449465, 3185835, 3608505, 3253185)).show(truncate = false)

    val r1 = df.where("rownum = 1")
      .withColumn("disclosure1", UdfStore.udfReplace(col("template"), col("key"), col("value")))
      .select(col("instablog_post_id"), col("disclosure_template_id"), col("disclosure1"))

    r1.where(col("instablog_post_id").isin(2771893, 3449465, 3185835, 3608505, 3253185)).show(truncate = false)

    val r2 = df.where("rownum = 2")
    val joined = r1.join(r2, Seq("instablog_post_id", "disclosure_template_id"), "left")
      .withColumn("disclosure", UdfStore.udfReplace(col("disclosure1"), col("key"), col("value")))
      .select(col("instablog_post_id"), col("disclosure"))
      .where(col("disclosure").isNotNull && length(col("disclosure")) > 0).distinct()
      .repartition(1)

    joined.where(col("instablog_post_id").isin(2771893, 3449465, 3185835, 3608505, 3253185)).show(truncate = false)

    joined
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/data/mariadb/content_disclosures/")

    joined
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", value = false)
      .save("/data/mariadb/content_disclosures/csv/")

    spark.stop()
  }
}
