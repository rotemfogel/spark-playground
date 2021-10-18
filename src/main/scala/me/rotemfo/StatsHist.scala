package me.rotemfo

import me.rotemfo.UdfStore.statsToDB
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object StatsHist extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = getSparkSession()
    val baseDir = "/data/user_pvs"
    val weenends: Seq[Int] = Range.inclusive(2, 6)

    val pageViewsDF = spark.read.parquet(s"$baseDir/user_unique_page_views/*")
      .withColumn("is_weekday", dayofweek(col("date_")).isin(weenends: _*).cast(IntegerType))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val articlesReadDF = spark.read.parquet(s"$baseDir/user_unique_articles_read/*")
      .withColumn("is_weekday", dayofweek(col("date_")).isin(weenends: _*).cast(IntegerType))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val lte1KPageViewsDF = pageViewsDF.where(col("unique_page_views").leq(lit(1000)))
    val lte1KArticlesReadDF = articlesReadDF.where(col("unique_articles").leq(lit(1000)))

    //   lessThan1KPageViewsDF.orderBy(desc("unique_page_views")).show(10)
    //   lessThan1KArticlesReadDF.orderBy(desc("unique_articles")).show(10)

    //    statsToDB(pageViewsDF, "page_views_total", "unique_page_views")
    //    statsToDB(articlesReadDF, "articles_total", "unique_articles")
    //    statsToDB(lessThan1KPageViewsDF, "page_views_1000", "unique_page_views")
    //    statsToDB(lessThan1KArticlesReadDF, "articles_1000", "unique_articles")

    statsToDB(pageViewsDF.where(col("is_weekday") === lit(1)), "page_views_weekdays_total", "unique_page_views")
    statsToDB(articlesReadDF.where(col("is_weekday") === lit(1)), "articles_weekdays_total", "unique_articles")
    statsToDB(lte1KPageViewsDF.where(col("is_weekday") === lit(1)), "page_views_weekdays_1000", "unique_page_views")
    statsToDB(lte1KArticlesReadDF.where(col("is_weekday") === lit(1)), "articles_weekdays_1000", "unique_articles")

    statsToDB(pageViewsDF.where(col("is_weekday") === lit(0)), "page_views_weekends_total", "unique_page_views")
    statsToDB(articlesReadDF.where(col("is_weekday") === lit(0)), "articles_weekends_total", "unique_articles")
    statsToDB(lte1KPageViewsDF.where(col("is_weekday") === lit(0)), "page_views_weekends_1000", "unique_page_views")
    statsToDB(lte1KArticlesReadDF.where(col("is_weekday") === lit(0)), "articles_weekends_1000", "unique_articles")

  }
}