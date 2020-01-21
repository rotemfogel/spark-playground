import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/**
 * project: spark-demo
 * package: 
 * file:    SparkReferrerCategory
 * created: 2019-10-17
 * author:  rotem
 */
object SparkReferrerCategory extends BaseSparkApp {

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession(cores = 16, memory = 12)

    import Schemas._

    val posts: DataFrame = spark.read.schema(schemaSrcPosts).json(postsDir: _*)

    import UdfStore._
    import org.apache.spark.sql.functions._
    val saRows = posts.filter(col(colNamePostsSrcEventsReqTime).isNotNull)
      .filter(col(colNamePostsReferrer).contains("seekingalpha"))
      .withColumn(colNameSessionKey, getSessionKey(col(colNamePostsSessionCookie)))
      .withColumn(colNameHasSourceParam, when(hasSourceParam(col(colNamePostsUrlParams)), 1).otherwise(0))
      .withColumn(colNameUnit, getReferrerPageCategory(col(colNamePostsReferrer)))

    val window = Window.partitionBy(col(colNameSessionKey)).orderBy(colNamePostsSrcEventsReqTime)
    val saWindow = saRows.withColumn(colNameLag, lag(col(colNamePostsReferrer), 1) over window)
    val isBack = saWindow.withColumn(colNameIsBack, when(col(colNamePostsUrl).contains(col(colNameLag)), 1).otherwise(0))

    FileUtils.deleteQuietly(new File(OUTPUT_DIR))
    //noinspection ScalaCustomHdfsFormat
    isBack.select(colNamePostsSrcEventsReqTime, colNameSessionKey, colNamePostsReferrer, colNamePostsUrl, colNamePostsUrlParams, colNameLag, colNameIsBack, colNameHasSourceParam)
      .write.format("com.databricks.spark.csv").option("header", value = true).save(OUTPUT_DIR)

    isBack.show()


    //    val backCountHasSource = isBack.select(colNameHasSourceParam, colNameReferrerPageCategory).groupBy(colNameHasSourceParam, colNameReferrerPageCategory).count().show()

    //    val colNameNewSessionKey = "new_session_key"
    //    val colNameCount = "count"
    //    val hasBack = isBack.groupBy(colNameSessionKey, colNameIsBack).count().filter(col(colNameCount) > 0)
    //      .withColumnRenamed(colNameSessionKey, colNameNewSessionKey).select(colNameNewSessionKey).distinct() //.as("t2")
    //
    //    val partitions = hasBack.collect().size
    //    val toWrite = isBack.join(hasBack, hasBack.col(colNameNewSessionKey).equalTo(isBack.col(colNameSessionKey)))
    //      .select(colNamePostsSrcEventsReqTime, colNameSessionKey, colNamePostsReferrer, colNamePostsUrl, colNamePostsUrlParams, colNameLag, colNameIsBack)
    //      .coalesce(partitions)
    //
    //    FileUtils.deleteQuietly(new File(OUTPUT_DIR))
    //    toWrite.write.format("com.databricks.spark.csv").option("header", true).save(OUTPUT_DIR)

    //    isBack.select(colNamePostsSrcEventsReqTime, colNameSessionKey, colNamePostsReferrer, colNamePostsUrl, colNamePostsUrlParams, colNameLag, colNameIsBack)
    //      .write.format("com.databricks.spark.csv").option("header", true).save(OUTPUT_DIR)

    //
    //    val sourceParams = saRows.withColumn("hasSourceParam", when(col(colNamePostsUrlParams).contains("source="), 1).otherwise(0))
    //    sourceParams.show
    //
    //    saRows.select(col(colNamePostsSessionCookie)).groupBy(col(colNamePostsSessionCookie)).count().orderBy(desc(colNameCount)).show
    //    val sourceParamCheck = sourceParams.groupBy("hasSourceParam").count()
    //    sourceParamCheck.show
    //
    //    val joined = events.join(posts, Seq(colNamePostsSrcEventsPageKey), "left")
    //
    //    joined.show()

    spark.stop()
  }
}
