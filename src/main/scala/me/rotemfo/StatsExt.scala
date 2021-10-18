package me.rotemfo

import me.rotemfo.UdfStore.{getJsonObjectNullSafe, ipToGeoJson}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, lit}

object StatsExt extends BaseSparkApp {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val baseDir = "/data/user_pvs"
    val userId = 53160350

    val df: DataFrame = spark.read.parquet(s"$baseDir/temp_user_scraping_pvs/*")
      .where(col("user_id") === lit(userId))
      .withColumn("geo", ipToGeoJson(col("machine_ip")))

    val uniqueFields = Seq("device_class", "device_brand", "machine_ip", "url")
    uniqueFields.foreach(f => {
      val c = df.select(col(f)).distinct.count
      println(s"$f -> $c")
    })

    val extractValues: Seq[String] = Seq("countryName", "region", "city", "postalCode", "continent")
    val geoDf = extractValues.foldLeft(df) {
      case (df, c) =>
        df.withColumn(c, getJsonObjectNullSafe(col("geo"), "$." + c))
    }

    // val selectColumns = "user_id" +: extractValues
    geoDf.groupBy(extractValues.head, extractValues.tail: _*)
      .count()
      .orderBy(desc("count"))
      .repartition(1)
      .write
      .option("header", "true")
      .format("csv")
      .save(s"$baseDir/$userId")
  }
}