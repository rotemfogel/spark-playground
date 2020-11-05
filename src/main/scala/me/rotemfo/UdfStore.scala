package me.rotemfo

import java.net.URLDecoder

import nl.basjes.parse.useragent.{UserAgent, UserAgentAnalyzer}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.{DefaultFormats, Extraction}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * project: spark-playground
 * package:
 * file:    UdfStore
 * created: 2019-11-19
 * author:  rotem
 */
object UdfStore {
  private final val HOME: String = "home"
  private final val logger: Logger = LoggerFactory.getLogger(getClass)

  private final lazy val seekingAlpha = "SeekingAlpha"
  private final lazy val seekingAlphaWebWrapper = s"com.${seekingAlpha.toLowerCase}.webwrapper"

  def qtrim: UserDefinedFunction = udf((s: String) => {
    s.replaceAll("\"", "").replaceAll("'", "")
  })

  private def _split(s: String): String = {
    if (s.contains("="))
      s.split("=").head.concat("=...")
    else s
  }

  def getSessionKey: UserDefinedFunction = udf((sessionCookie: String) => {
    sessionCookie.split("\\.").head
  })

  private final lazy val userAgentAnalyzer: UserAgentAnalyzer = UserAgentAnalyzer.newBuilder().build()

  private def printUserAgent(ua: UserAgent): Unit = {
    import scala.collection.JavaConverters._
    logger.debug(ua.getAvailableFieldNames.asScala.map(f => f -> ua.getValue(f)).sorted.mkString("\n"))
  }

  private lazy val regex = "^sa-.*-wrapper$".r

  private def saParseUserAgent(userAgentString: String): Map[String, String] = {
    try {
      if (userAgentString.contains(seekingAlpha)) {
        val parts = userAgentString.split(";")
        // old iOS App
        val last = parts.last.split(" ")
        val os = last(2).split("/")
        val osName = {
          val v = os(0).toLowerCase
          if (v.equals("ios")) "iOS"
          else if (v.equals("android")) "Android"
          else os(0)
        }
        val m = Map(colNameUserAgentOsName -> osName, colNameUserAgentOsVersion -> os(1))
        // old iOS App
        if (last.length == 7) {
          val agentVersion: String = last(5).split("/").last
          val agentName: String = last(6).split("/").last
          m ++ Map(colNameUserAgentAgentName -> agentName, colNameUserAgentAgentVersion -> agentVersion)
        }
        else m
      }
      else if (regex.findFirstIn(userAgentString).isDefined || userAgentString.startsWith(seekingAlphaWebWrapper)) {
        val parts = userAgentString.split(";")
        val os = parts(2).trim.split(" ")
        if (os.length == 3) Array(os(0), os(2))
        else Array(os(0), os(1))
        Map(colNameUserAgentOsName -> os(0), colNameUserAgentOsVersion -> os(1))
      }
      else Map()
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.error(s"error parsing user_agent: $userAgentString", e)
        Map()
    }
  }

  import scala.collection.JavaConverters._

  /**
   * parse user agent
   *
   * @param str the user agent str
   * @param ua  the user agent analyzer
   * @return optional json string representing parse user agent map
   */
  def userAgentParser(str: String, ua: Broadcast[UserAgentAnalyzer]): Option[String] = {
    if (StringUtils.isEmpty(str)) None
    else {
      val agent: UserAgent = ua.value.parse(str)
      val map: Map[String, String] = {
        val m: Map[String, String] =
          agent.getAvailableFieldNames.asScala
            .map(x => {
              val value: String = {
                val v: String = agent.getValue(x)
                if (x.equals(colNameUserAgentOsVersion) || x.equals(colNameUserAgentAgentVersion))
                  v.split(" ").last
                else v
              }
              (x, value)
            })
            .toMap[String, String]
        val os = m(colNameUserAgentOsName)
        if (os.toLowerCase.startsWith("unknown") || os.contains("??")) {
          val sa: Map[String, String] = saParseUserAgent(str)
          mergeMaps(m, sa)
        }
        else m
      }
      val m = map.filter({ case (_, v) => !v.equals("??") || !v.toLowerCase.startsWith("unknown") })
      Some(toJson(m))
    }
  }

  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   * util function to extract T type from json string
   *
   * @param s the jsong string
   * @return T
   */
  def fromJson[T](s: String)(implicit m: Manifest[T]): T = org.json4s.jackson.parseJson(s).extract[T]

  /**
   * util function to create a json string from any
   *
   * @param a any object
   * @return json string
   */
  def toJson(a: Any): String = compact(render(Extraction.decompose(a)))

  def mergeMaps[K, V](m1: Map[K, V], m2: Map[K, V]): Map[K, V] = {
    if (m2.isEmpty) m1
    else (m1.keySet ++ m2.keySet).map(k => if (m2.contains(k)) (k, m2(k)) else (k, m1(k))).toMap
  }

  def hasSourceParam: UserDefinedFunction = udf((urlParams: String) => {
    if (StringUtils.isEmpty(urlParams)) false
    else {
      urlParams.trim.replaceAll("\\?", "")
        .split("&")
        .map(t => t.split("="))
        .map(p => (p.head, p.last))
        .toMap
        .contains("source")
    }
  })

  def getReferrerPageCategory: UserDefinedFunction = udf((referrer: String) => {
    try {
      if (StringUtils.isEmpty(referrer)) HOME
      else {
        val url = try {
          URLDecoder.decode(referrer, "UTF-8")
        } catch {
          case _: Throwable => referrer
        }
        val head: String = url.split("\\?").head
        val parts: Array[String] = head.split("//").last.split("seekingalpha\\.com").last.split("/")
        if (parts.length == 0) HOME
        else if (parts.length == 1) {
          if (parts.head.contains("/")) parts.head.split("/").last
          else parts.head
        } else {
          parts(1) match {
            case "account" => try {
              _split(parts(2))
            } catch {
              case _: ArrayIndexOutOfBoundsException => parts(1)
            }
            case _ => try {
              _split(parts(1))
            } catch {
              case _: ArrayIndexOutOfBoundsException => HOME
            }
          }
        }
      }
    } catch {
      case _: Throwable =>
        HOME
    }
  })

  def calcLagPercent: UserDefinedFunction = udf((count: Int, lag: Int) => {
    if (lag == 0) 100.0
    else (count - lag).toDouble / lag.toDouble
  })


  def udfReplace: UserDefinedFunction = udf((template: String, key: String, value: String) => {
    if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) template
    else Try(template.replace(key, value)) match {
      case Success(rep) => rep
      case Failure(_) => template
    }
  })

  implicit class DataFrameExSql(dataFrame: DataFrame) {
    def cast(schema: StructType): DataFrame = {
      schema.foldLeft(dataFrame) { case (df, c) =>
        df.withColumn(s"_${c.name}", col(c.name).cast(c.dataType))
          .drop(c.name)
          .withColumnRenamed(s"_${c.name}", c.name)
      }
    }
  }

}