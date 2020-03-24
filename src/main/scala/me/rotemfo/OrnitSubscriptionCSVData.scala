package me.rotemfo

import java.io.{File, FileFilter, FileReader, FileWriter}

import com.opencsv.{CSVReader, CSVWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * project: spark-demo
 * package:
 * file:    SparkReferrerCategory
 * created: 2019-10-17
 * author:  rotem
 */
//noinspection ScalaCustomHdfsFormat,ScalaCustomHdfsFormat
object OrnitSubscriptionCSVData extends BaseSparkApp {

  private final val FILE_NAME = "user_registration_tracking.csv"

  private final val schema = StructType(
    List(
      StructField("user_id", LongType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("registration_date", DateType, nullable = true),
      StructField("registration_hour", IntegerType, nullable = true),
      StructField("confirmation_date", DateType, nullable = true),
      StructField("is_confirmed", IntegerType, nullable = true),
      StructField("user_agent", StringType, nullable = true),
      StructField("is_gplus", LongType, nullable = true),
      StructField("page_type", StringType, nullable = true),
      StructField("client_id", StringType, nullable = true)
    )
  )

  def main(args: Array[String]): Unit = {
    val path = "/tmp"

    val spark = getSparkSession(cores = 16, memory = 12)

    val df = spark.read
      .schema(schema)
      .format(DATABRICKS_CSV)
      .option("header", value = true)
      .option("inferSchema", value = false)
      .option("delimiter", ",")
      .csv(path + File.separator + FILE_NAME)
      .withColumn(colNamePostsUserAgentJson, UdfStore.udfUserAgent(col(colNamePostsUserAgent)))
      .withColumn(colNamePostsUserAgentStruct, from_json(col(colNamePostsUserAgentJson), userAgentSchema))
      .drop(colNamePostsUserAgentJson)
      .withColumn(colNameEventsDeviceClass, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentDeviceClass"))
      .withColumn(colNameEventsAgentClass, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentAgentClass"))
      .withColumn(colNameEventsOsName, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentOsName"))
      .withColumn(colNamEventsOsVersion, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentOsVersion"))
      .withColumn(colNameEventsAgentName, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentAgentName"))
      .withColumn(colNameEventsAgentVersion, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentAgentVersion"))
      .withColumn(colNameEventsDeviceBrand, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentDeviceBrand"))
      .withColumn(colNameEventsDeviceName, col(s"$colNamePostsUserAgentStruct.$colNameUserAgentDeviceName"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val output = df
      .select(
        col("user_id"),
        col("email"),
        col("registration_date"),
        col("registration_hour"),
        col("confirmation_date"),
        col("is_confirmed"),
        col("is_gplus"),
        col("page_type"),
        col(colNameEventsDeviceClass),
        col(colNameEventsOsName),
        col(colNamEventsOsVersion),
        col(colNameEventsAgentName),
        col(colNameEventsAgentVersion),
        col(colNameEventsDeviceBrand),
        col(colNameEventsDeviceName),
        col("client_id")
      )
      .filter(col(colNameEventsAgentClass).notEqual("Browser Webview"))
      // .filter(not(col("agent_class").eqNullSafe("Mobile App")))
      .orderBy(col("registration_date"), col("registration_hour"), col("email"))

    // userAgentDf.select(col("client_id")).distinct().show()
    val outputDir = OUTPUT_DIR + "/registration"
    FileUtils.deleteQuietly(new File(outputDir))

    //    new File(outputDir).mkdirs()
    //    val pw = new PrintWriter(new FileOutputStream(new File(outputDir + "/" + FILE_NAME)))
    //    pw.println(schema.map(_.name).mkString(","))
    //
    //    val list = schema.fields.zipWithIndex.map(r => (r._2 + 1, r._1)).sortBy(_._1)
    //    userAgentDf.collect().foreach(row => {
    //      val values = list.map(t => {
    //        val v = t._2.dataType match {
    //          case DateType => row.getTimestamp(t._1)
    //          case StringType => {
    //            val s = row.getString(t._1)
    //            if (t._2.name.equals("client_id")) "\"" + s + "\"" else s
    //          }
    //          case IntegerType => row.getInt(t._1)
    //          case LongType => row.getLong(t._1)
    //          case _ => row.get(t._1).toString
    //        }
    //      }).toSeq
    //      pw.println(values.mkString(","))
    //    })
    //    pw.close()

    output
      .coalesce(1)
      .write
      .format(DATABRICKS_CSV)
      .option("header", value = true).save(outputDir)

    spark.stop()

    val files = new File(outputDir).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.split("\\.").last.equals("csv")
    })
    val reader = new CSVReader(new FileReader(files.head))
    val lines = reader.readAll()
    reader.close()

    val writer = new CSVWriter(new FileWriter(files.head))
    writer.writeAll(lines)
    writer.close()
  }

}
