package me.rotemfo

import java.io.File
import java.sql.Timestamp

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, NotFileFilter, TrueFileFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Sessionize extends Logging {

  import scala.collection.JavaConverters._

  private def getDirectoryTree(path: String): List[String] = {
    val list = FileUtils.listFilesAndDirs(new File(path), new NotFileFilter(TrueFileFilter.INSTANCE), DirectoryFileFilter.DIRECTORY).asScala.map(_.getAbsolutePath).toList
    log.info("{}", list)
    list
  }

  def sessionization(dataFrame: DataFrame, userIdentifierColumn: String = "user_id", timestampColumn: String = "timestamp_", sessionEnumeratorName: String = "user_session", sessionLengthDefinition: String = "INTERVAL 30 SECOND"): DataFrame = {
    val identifierWindowSpec = Window
      .partitionBy(userIdentifierColumn)
      .orderBy(timestampColumn)
    val sessionWindowSpec = Window
      .partitionBy(userIdentifierColumn, sessionEnumeratorName)
    val pageKeyWindowSpec = Window
      .partitionBy(userIdentifierColumn, sessionEnumeratorName, "page_key")
    val lagTimestampColumn = "lag_" + timestampColumn
    dataFrame
      .withColumn(lagTimestampColumn, lag(timestampColumn, 1).over(identifierWindowSpec))
      .withColumn(sessionEnumeratorName, count(
        when(col(timestampColumn) > col(lagTimestampColumn) + expr(sessionLengthDefinition), 1)
          .otherwise(null))
        .over(identifierWindowSpec))
      .withColumn("session_max_timestamp", max(timestampColumn).over(sessionWindowSpec))
      .withColumn("page_key_count", count("page_key").over(pageKeyWindowSpec))
      .withColumn("is_bounce", when(col("page_key_count") === 1, true).otherwise(false))
      .withColumn("for_session_max", when(col("is_bounce") === false, col(timestampColumn) + expr(sessionLengthDefinition)).otherwise(col(timestampColumn)))
      .withColumn("for_session_min", coalesce(col("prev_session_min_timestamp"), col(timestampColumn)))
      .groupBy(userIdentifierColumn, sessionEnumeratorName)
      .agg(
        min("for_session_min").as("session_min_timestamp"),
        max("for_session_max").as("session_max_timestamp")
      )
  }

  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("my-app")
      .master("local[*]")
      .getOrCreate()
    val simpleData = Seq(
      (1, "2009-12-08 15:00:05", "1a", 0),
      (2, "2009-12-08 15:00:15", "2a", 0),
      (2, "2009-12-08 15:00:35", "2a", 0),
      (2, "2009-12-08 15:01:05", "2b", 0),
      (2, "2009-12-08 15:01:45", "2b", 1),
      (2, "2009-12-08 15:02:55", "2c", 2),
      (2, "2009-12-08 15:03:55", "2d", 3),
      (2, "2009-12-08 15:04:05", "2d", 3),
      (2, "2009-12-08 15:04:15", "2d", 3),
      (2, "2009-12-08 15:04:45", "2e", 3),
      (4, "2009-12-08 15:00:35", "4a", 0),
      (4, "2009-12-08 15:00:55", "4a", 0),
      (4, "2009-12-08 15:01:35", "4a", 1),
      (4, "2009-12-08 15:01:45", "4b", 1),
      (4, "2009-12-08 15:02:55", "4c", 2)
    )
    val simpleDateTs = simpleData.map(x => Row(x._1, Timestamp.valueOf(x._2), x._3, x._4))
    val dataSchema = StructType(
      Seq(
        StructField("user_id", IntegerType, nullable = false),
        StructField("timestamp_", TimestampType, nullable = false),
        StructField("page_key", StringType, nullable = false),
        StructField("cte", IntegerType, nullable = false)
      )
    )
    import scala.collection.JavaConversions._
    val df = spark.createDataFrame(simpleDateTs, dataSchema)
      .withColumn("prev_session_min_timestamp", lit(null.asInstanceOf[Timestamp]))
    //        .withColumn("sha1_hash", sha1(concat(col("userid"), col("cte"))))
    df.show()

    val df_session = sessionization(df)
      .select("user_id", "session_min_timestamp", "session_max_timestamp")

    df_session.show()

    val df_session_prev = df_session
      .where(col("session_max_timestamp") + expr("INTERVAL 30 SECOND") > Timestamp.valueOf("2009-12-08 15:05:00"))
      .withColumnRenamed("session_max_timestamp", "timestamp_")
      .withColumnRenamed("session_min_timestamp", "prev_session_min_timestamp")
      .withColumn("cte", lit(0))
      .withColumn("page_key", lit(null.asInstanceOf[String]))
      .select("user_id", "timestamp_", "page_key", "cte", "prev_session_min_timestamp")
    df_session_prev.show()
    //    Next window
    val nextSimpleData = Seq(
      Row(2, Timestamp.valueOf("2009-12-08 15:05:05"), "2e", 0),
      Row(2, Timestamp.valueOf("2009-12-08 15:05:20"), "2f", 0),
      Row(2, Timestamp.valueOf("2009-12-08 15:05:55"), "2f", 1),
      Row(2, Timestamp.valueOf("2009-12-08 15:06:05"), "2h", 1),
      Row(4, Timestamp.valueOf("2009-12-08 15:05:35"), "4c", 0),
      Row(4, Timestamp.valueOf("2009-12-08 15:06:45"), "4c", 1),
      Row(4, Timestamp.valueOf("2009-12-08 15:06:55"), "4d", 1)
    )
    val df_next = spark.createDataFrame(nextSimpleData, dataSchema)
      .withColumn("prev_session_min_timestamp", lit(null.asInstanceOf[Timestamp]))
      .union(df_session_prev)
    df_next.show()
    sessionization(df_next).show()
    spark.stop()
  }
}
