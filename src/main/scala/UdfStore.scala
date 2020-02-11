import java.net.URLDecoder

import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * project: spark-playground
 * package: 
 * file:    UdfStore
 * created: 2019-11-19
 * author:  rotem
 */
object UdfStore {
  private final val HOME: String = "home"

  private def _split(s: String): String = {
    if (s.contains("="))
      s.split("=").head.concat("=...")
    else s
  }

  def getSessionKey: UserDefinedFunction = udf((sessionCookie: String) => {
    sessionCookie.split("\\.").head
  })

  private final lazy val userAgentAnalyzer: UserAgentAnalyzer = UserAgentAnalyzer.newBuilder().build()
/*
  private def printUserAgent(ua: UserAgent): Unit = {
    import scala.collection.JavaConverters._
    println(compact(render(Extraction.decompose(ua.getAvailableFieldNames.asScala.map(f => (f -> ua.getValue(f))).toMap))))
  }
*/
  def udfUserAgent: UserDefinedFunction = udf((userAgentString: String) => {
    val ua = userAgentAnalyzer.parse(userAgentString)
    // printUserAgent(ua)
    ua.getValue("OperatingSystemName") + "," + ua.getValue("AgentNameVersion") + "," + ua.getValue("DeviceClass") + "," + ua.getValue("AgentClass")
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
