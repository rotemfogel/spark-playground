package me.rotemfo.oo

import me.rotemfo.oo.BaseApplication.dateHourPatternFormatter
import me.rotemfo.oo.BaseParameters.{EST, UTC, defaultStorageLevel}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.TimeZone
import scala.util.{Failure, Try}

object BaseParameters {
  val defaultStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER
  val UTC: String = "UTC"
  val EST: String = "America/New_York"
}

trait BaseParameters {
  val dateHourInputs: Seq[LocalDateTime] = Seq.empty[LocalDateTime]
  val outputLocation: Option[String] = None
  val sparkConfig: Map[String, String] = Map.empty[String, String]
  val storageLevel: StorageLevel = defaultStorageLevel
  val defTimeZone: String = UTC
  val toTimeZone: String = EST
  val checkReadPaths: Boolean = false
}

// @formatter:off
trait SparkApp[T <: BaseParameters] extends Logging {
  protected def processWith(implicit p: T, spark: SQLContext): Unit
  protected def getParser: OptionParser[T]
  protected def configSparkSession(p: T, spark: SparkSession.Builder): Unit = {
    p.sparkConfig.foreach(config => {
      spark.config(config._1, config._2)
    })
  }
}
// @formatter:on

abstract class BaseApplication[T <: BaseParameters](t: T) extends SparkApp[T] {
  def main(args: Array[String]): Unit = {
    getParser.parse(args, t).foreach { p =>
      log.info(p.toString)

      val sparkSessionBuilder = SparkSession.builder()
        .enableHiveSupport()
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.broadcastTimeout", 6600)
      // .config("spark.sql.files.ignoreCorruptFiles", "true")


      configSparkSession(p, sparkSessionBuilder)

      val sparkSession = sparkSessionBuilder.getOrCreate
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
      hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
      hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")

      val processResults: Try[Unit] = Try(processWith(p, sparkSession.sqlContext))
      sparkSession.sparkContext.getPersistentRDDs.values.foreach(_.unpersist())
      sparkSession.close()
      processResults match {
        case Failure(ex) => throw ex
        case _ =>
      }
    }
  }
}

object BaseApplication {
  val dateHourPattern = "yyyy-MM-dd-HH"
  val dateHourPatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateHourPattern)
  val datePattern = "yyyy-MM-dd"
  val datePatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern)
  val hourPattern = "HH"
  val hourPatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(hourPattern)
  val datetimeSQLPattern = "yyyy-MM-dd HH:mm:ss.[SSSSSS]"
  val datetimeSQLFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datetimeSQLPattern)
}

// ------------------------------------------------------------------------------------------------------

case class PageEventParameters(override val dateHourInputs: Seq[LocalDateTime] = Seq.empty[LocalDateTime],
                               override val outputLocation: Option[String] = None,
                               override val sparkConfig: Map[String, String] = Map.empty[String, String],
                               override val storageLevel: StorageLevel = defaultStorageLevel,
                               override val defTimeZone: String = UTC,
                               override val toTimeZone: String = EST,
                               override val checkReadPaths: Boolean = false,
                               enrichmentDurationInput: Duration = Duration.parse("PT24H"),
                               dbName: Option[String] = None,
                               writeRepartitionPageViews: Int = 4,
                               writeRepartitionPageEvents: Int = 4,
                               pageEventsEnrichment: Option[Seq[String]] = None,
                               monePostsTable: Option[String] = None,
                               moneEventsTable: Option[String] = None,
                              ) extends BaseParameters

// @formatter:off
object PageEventParametersParser extends OptionParser[PageEventParameters](programName = "PageEvent Application") {
  opt[String]("output-path-event")    .required.action { (x, p) => p.copy(outputLocation = Some(x)) }
  opt[Seq[String]]("date-hour-inputs").required.action { (x, p) => p.copy(dateHourInputs = x.map(LocalDateTime.parse(_, dateHourPatternFormatter))) }
    .validate(x => if (x.length == 1) success else if (x.head <= x.tail.head) success else failure("start date is after end date"))
  opt[String]("enrichment-duration-input")     .action { (x, p) => p.copy(enrichmentDurationInput = Duration.parse(x)) }
  opt[String]("def-timezone")                  .action { (x, p) => p.copy(defTimeZone = x) }
  opt[String]("to-timezone")                   .action { (x, p) => p.copy(toTimeZone = x) }
  opt[String]("db-name")              .required.action { (x, p) => p.copy(dbName = Some(x)) }
  opt[Int]("write-repartition-page-views")     .action { (x, p) => p.copy(writeRepartitionPageViews = x) }
  opt[Int]("write-repartition-page-events")    .action { (x, p) => p.copy(writeRepartitionPageEvents = x) }
  opt[Unit]("check-read-paths")                .action { (_, p) => p.copy(checkReadPaths = true) }
  opt[String]("mone-posts-table")     .required.action { (x, p) => p.copy(monePostsTable = Some(x)) }
  opt[String]("mone-events-table")    .required.action { (x, p) => p.copy(moneEventsTable = Some(x)) }
  opt[String]("storage-level")                 .action { (x, p) => p.copy(storageLevel = StorageLevel.fromString(x.toUpperCase)) }
}
// @formatter:on

object PageEvent extends BaseApplication[PageEventParameters](PageEventParameters()) {
  override protected def processWith(implicit p: PageEventParameters, spark: SQLContext): Unit = {
    // Data TZ will converted from UTC to NY, we will drop 4 hours of the oldest date and add 5 hours for the newest date (TZ NY -4/-5 hours)
    TimeZone.setDefault(TimeZone.getTimeZone(p.defTimeZone)) // set machine TZ to UTC (UTC is AWS EMR TZ)

    val dateHourEnrichmentInputs: Seq[LocalDateTime] = Seq(p.dateHourInputs.head.minus(p.enrichmentDurationInput), p.dateHourInputs.last)

    log.info(s"about to filter posts from ${dateHourEnrichmentInputs.head} to ${dateHourEnrichmentInputs.last}")
  }

  override protected def getParser: OptionParser[PageEventParameters] = PageEventParametersParser
}