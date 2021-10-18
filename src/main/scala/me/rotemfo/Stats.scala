package me.rotemfo

import me.rotemfo.UdfStore.statsToDB
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Stats extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = getSparkSession()
    val baseDir = "/data/user_device"

    val df = spark.read.parquet(s"$baseDir/*").persist(StorageLevel.MEMORY_AND_DISK)
    //    val dates: Seq[Date] = df.select("date_").distinct.collect().map(d => d.getDate(0))
    //
    //    implicit def ordered: Ordering[Date] = new Ordering[Date] {
    //      def compare(x: Date, y: Date): Int = x.compareTo(y)
    //    }
    //
    //    dates.sorted.foreach(d => statsToDB(df, "device_id", Some(d)))
    statsToDB(df, "total", "device_id", None)
  }
}