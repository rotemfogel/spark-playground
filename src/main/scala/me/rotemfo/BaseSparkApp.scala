package me.rotemfo

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, NotFileFilter, TrueFileFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * project: spark-demo
 * package:
 * file:    BaseSparkApp
 * created: 2019-11-05
 * author:  rotem
 */
trait BaseSparkApp extends Logging {
  protected final val HOME_DIR = s"${System.getProperty("user.home")}/"
  protected final val BASE_DIR = s"${HOME_DIR}dev/data/"
  protected final val OUTPUT_DIR = s"${BASE_DIR}results"
  protected final val MONE_POSTS = "mone_posts/"
  protected final val MONE_EVENTS = "mone_events/"
  protected final val DATE_DIR = "2019/10/"

  protected val colNameSessionKey = "session_key"
  protected val colNameLag = "lag"
  protected val colNameIsBack = "is_back"
  protected val colNameHasSourceParam = "has_source_param"
  protected val colNameUnit = "unit"
  protected val colNameCount = "count"
  protected val colNameClientTime = "client_time"
  protected val colNameTimezone = "timezone"

  private final val maxMemory = (Runtime.getRuntime.maxMemory() / 1024 / 1024 / 1024).intValue()
  private final val maxCores = Runtime.getRuntime.availableProcessors()

  import scala.collection.JavaConverters._

  protected lazy val postsDir: List[String] = getDirectoryTree(s"$BASE_DIR$MONE_POSTS$DATE_DIR")

  protected def getDirectoryTree(path: String): List[String] = {
    val list = FileUtils.listFilesAndDirs(new File(path), new NotFileFilter(TrueFileFilter.INSTANCE), DirectoryFileFilter.DIRECTORY).asScala.map(_.getAbsolutePath).toList
    log.info("{}", list)
    list
  }

  protected def getSparkSession(appName: String = getClass.getSimpleName, cores: Int = maxCores, memory: Int = maxMemory / maxCores, params: Map[String, Any] = Map()): SparkSession = {
    require(memory < maxMemory, s"cannot allocate $memory > $maxMemory")
    val builder = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .config("spark.executor.cores", cores)
      .config("spark.executor.memory", s"${memory}g")
    params.foreach({ case (k, v) => builder.config(k, v.toString) })
    builder.getOrCreate()
  }
}
