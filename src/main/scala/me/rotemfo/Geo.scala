package me.rotemfo

import me.rotemfo.UdfStore.ipToGeoJson
import org.apache.spark.sql.functions.{col, countDistinct, get_json_object, hash}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object Geo extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = getSparkSession()
    val df = spark.read.parquet("/data/user_machine_ip").persist(MEMORY_AND_DISK)
    val geo = df.withColumn("geo_ip_json", ipToGeoJson(col("machine_ip")))
      .withColumn("country_geo", get_json_object(col("geo_ip_json"), "$.country_geo"))
      .withColumn("territory_geo", get_json_object(col("geo_ip_json"), "$.territory_geo"))
      .persist(MEMORY_AND_DISK)
    geo
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/data/user_geo")

    geo.withColumn("geo_id", hash(col("country_geo"), col("territory_geo")))
      .groupBy("date_", "user_id")
      .agg(countDistinct("geo_id").as("geo_id"))
      .persist(MEMORY_AND_DISK)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/data/user_geo_agg")

    //    val dff = spark.read.parquet("/data/user_geo")
    //    statsToDB(dff, "total", "geo_id", None)
  }
}