package me.rotemfo

import java.net.URLDecoder

import nl.basjes.parse.useragent.{UserAgent, UserAgentAnalyzer}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.slf4j.{Logger, LoggerFactory}

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

  private def saParseUserAgent(userAgentString: String): Map[String, String] = {
    try {
      val arr = if (userAgentString.contains("SeekingAlpha")) {
        val parts = userAgentString.split(";")
        val os = parts.last.split(" ")
        os(2).split("/")
      }
      else {
        val parts = userAgentString.split(";")
        val os = parts(2).trim.split(" ")
        if (os.length == 3) Array(os(0), os(2))
        else Array(os(0), os(1))
      }
      Map(colNameUserAgentOsName -> arr.head, colNameUserAgentAgentVersion -> arr.last)
    } catch {
      case _: ArrayIndexOutOfBoundsException => Map()
    }
  }

  import scala.collection.JavaConverters._

  def userAgentParser(str: String): Map[String, String] = {
    if (StringUtils.isEmpty(str)) Map.empty[String, String]
    else {
      val agent: UserAgent = userAgentAnalyzer.parse(str)
      val map: Map[String, String] = {
        val m: Map[String, String] =
          agent.getAvailableFieldNames.asScala
            // .filter(x => !(agent.getValue(x).trim  == "??" || agent.getValue(x).toLowerCase.trim  == "unknown"|| agent.getValue(x).toLowerCase.trim  == "unknown ??"))
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
          m ++ saParseUserAgent(str)
        }
        else m
      }
      map.filter({ case (k, v) => !v.equals("??") || !v.toLowerCase.startsWith("unknown") })
    }
  }

  def udfUserAgent: UserDefinedFunction = udf((userAgentString: String) => {
    userAgentParser(userAgentString)
  })

  def hasSourceParam: UserDefinedFunction = udf((urlParams: String) => {
    if (StringUtils.isEmpty(urlParams)) false
    else {
      urlParams.trim.replaceAll("\\?", "")
        .split("&")
        .map(t => t.split("="))
        .map(p => (p.head, p.last))
        .toMap
        .get("source").isDefined
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

  /*
    implicit val formats: DefaultFormats.type = DefaultFormats

    import org.json4s.native.JsonMethods._

    def parseJson: UserDefinedFunction = udf((s: String) => {
      val map: Map[String, Any] = parse(s).values.asInstanceOf[Map[String, Any]]
      println(map.keySet.mkString(","))
      val map2 = map.filterKeys(_ != "portfolio_id")
      println(map2.keySet.mkString(","))
      compact(render(Extraction.decompose(map2)))
    })
  */
}
