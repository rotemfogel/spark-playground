package me.rotemfo

import org.apache.spark.sql.DataFrame

/**
 * project: spark-demo
 * package:
 * file:    SparkCountSource
 * created: 2019-10-17
 * author:  rotem
 */
object SparkCountSource extends BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession(cores = 16, memory = 12)

    import Schemas._

    val posts: DataFrame = spark.read.schema(schemaSrcPosts).json(postsDir: _*)

    import UdfStore._
    import org.apache.spark.sql.functions._
    val saRows = posts.filter(col(colNamePostsSrcEventsReqTime).isNotNull)
      .filter(col(colNamePostsReferrer).contains("seekingalpha"))
      .withColumn(colNameUnit, getReferrerPageCategory(col(colNamePostsReferrer)))
      .withColumn(colNameHasSourceParam, when(hasSourceParam(col(colNamePostsUrlParams)), 1).otherwise(0))

    val saCountHasSource: DataFrame = saRows.select(colNameUnit, colNameHasSourceParam)
      // .where(col(colNameHasSourceParam).equalTo(lit(0)))
      .groupBy(colNameUnit, colNameHasSourceParam)
      .count()
      .cache()

    val rows = saCountHasSource.count()

    saCountHasSource
      .sort(desc(colNameCount))
      .show(rows.toInt)

    spark.stop()
  }
}
