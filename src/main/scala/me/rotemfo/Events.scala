package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Events {
  val columns: Array[String] = Array(
    "ad_creative",
    "ad_position",
    "contextual_key",
    "experience",
    "version",
    "banner_location",
    "new_user",
    "variation",
    "identifier",
    "machine_cookie_session_id",
    "paying_product_map",
    "free_trial_product_map"
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]").getOrCreate()
    var df = spark.read.parquet("/data/spark/*.parquet")
    columns.foreach(e => {
      df = df.withColumn(s"${e}_len", length(col(e)))
    })
    df.select("ad_creative_len", "ad_position_len", "contextual_key_len", "experience_len", "version_len", "banner_location_len", "new_user_len", "variation_len", "identifier_len")
      .agg(max("ad_creative_len"), max("ad_position_len"), max("contextual_key_len"), max("experience_len"), max("version_len"), max("banner_location_len"), max("new_user_len"), max("variation_len"), max("identifier_len"))
      .show()
    //    df.select(col())
  }
}
