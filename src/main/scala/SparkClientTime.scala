import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.TimeZone

import com.sanoma.cda.geoip.{IpLocation, MaxMindIpGeo}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * project: spark-demo
 * package: 
 * file:    SparkClientTime
 * created: 2019-10-17
 * author:  rotem
 */
//noinspection ScalaUnusedSymbol
object SparkClientTime extends BaseSparkApp {

  import org.apache.spark.sql.functions._

  private final val geoIp = MaxMindIpGeo("/opt/maxmind/GeoLite2-City.mmdb", 10000)

  private def ipLookup(ip: String): Option[IpLocation] = geoIp.getLocationWithLruCache(ip)

  private def dateToTimezone(reqTime: Timestamp, zone: String): Timestamp = {
    val dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(reqTime.toInstant.toEpochMilli), ZoneId.of(zone))
    Timestamp.valueOf(dt)
  }

  private def getClientTimeByZone: UserDefinedFunction = udf((reqTime: Timestamp, zone: String) => {
    dateToTimezone(reqTime, zone)
  })

  private def getClientTimeByIp: UserDefinedFunction = udf((reqTime: Timestamp, ip: String) => {
    try {
      val result = ipLookup(ip)
      if (result.isEmpty) {
        reqTime
      } else {
        val tz = result.get.timezone
        if (tz.isEmpty) reqTime
        else {
          dateToTimezone(reqTime, tz.get)
        }
      }
    } catch {
      case _: Throwable => reqTime
    }
  })

  private final val defaultZone = ZoneId.of("America/New_York")

  private def getTimezone: UserDefinedFunction = udf((reqTime: Timestamp, ip: String) => {
    try {
      val result = ipLookup(ip)
      if (result.isEmpty) defaultZone.toString
      else {
        val tz = result.get.timezone
        if (tz.isDefined) tz.get else defaultZone.toString
      }
    } catch {
      case _: Throwable => defaultZone.toString
    }
  })

  def main(args: Array[String]): Unit = {

    TimeZone.setDefault(TimeZone.getTimeZone(defaultZone))

    val spark = getSparkSession(cores = 16, memory = 12)

    import Schemas._

    val posts: DataFrame = spark.read.schema(schemaSrcPosts).json(postsDir: _*)

    import org.apache.spark.sql.functions._

    val saRows = posts.filter(col(colNamePostsSrcEventsReqTime).isNotNull)
      .filter(col(colNamePostsReferrer).contains("seekingalpha"))
      .withColumn(colNameTimezone, getTimezone(col(colNamePostsSrcEventsReqTime), col(colNameEventsMachineIp)))
      .withColumn(colNameClientTime, when(col(colNameEventsMachineIp).isNotNull, getClientTimeByIp(col(colNamePostsSrcEventsReqTime), col(colNameEventsMachineIp))).otherwise(col(colNamePostsSrcEventsReqTime)))

    saRows.show
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
