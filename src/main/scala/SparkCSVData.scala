import java.io.{File, FileOutputStream, PrintWriter}

import UdfStore._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * project: spark-demo
 * package: 
 * file:    SparkReferrerCategory
 * created: 2019-10-17
 * author:  rotem
 */
object SparkCSVData extends BaseSparkApp {

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
    val _path = SparkCSVData.getClass.getResource("/").getPath
    val path = (_path.split(File.separator).dropRight(3) ++ Array("src", "main", "resources")).mkString(File.separator)

    val spark = getSparkSession(cores = 16, memory = 12)

    val data = spark.read
      .schema(schema)
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = false)
      .option("delimiter", ",")
      .csv(path + File.separator + FILE_NAME)

    val userAgentDf = data
      .withColumn("user_agent_json", udfUserAgent(col("user_agent")))
      .withColumn("_tmp", split(col("user_agent_json"), ","))
      .select(
        col("user_id"),
        col("email"),
        col("registration_date"),
        col("registration_hour"),
        col("confirmation_date"),
        col("is_confirmed"),
        col("is_gplus"),
        col("page_type"),
        col("_tmp").getItem(0).as("os_name"),
        col("_tmp").getItem(1).as("browser_name"),
        col("_tmp").getItem(2).as("device_class"),
        col("_tmp").getItem(3).as("agent_class"),
        col("client_id")
      )
      .filter(col("agent_class").notEqual("Browser Webview"))
      // .filter(not(col("agent_class").eqNullSafe("Mobile App")))
      .orderBy(col("registration_date"), col("registration_hour"), col("email"))

    // userAgentDf.select(col("client_id")).distinct().show()
    val outputDir = OUTPUT_DIR + "/registration"
    FileUtils.deleteQuietly(new File(outputDir))
    new File(outputDir).mkdirs()

    def leave(s: String => String) = s
    def convert(s: String => String) = "\"" + s + "\""

    val pw = new PrintWriter(new FileOutputStream(new File(outputDir + "/" + FILE_NAME)))
    schema.foreach(s => pw.write(s.name + ","))
    pw.println("")
    userAgentDf.collect().foreach(row => {
      schema.foreach(f => {
        row.get
        pw.write(v)
      })
      pw.println
    })
    pw.close()
//
//    userAgentDf
//      .coalesce(1)
//      .write
//      .format("com.databricks.spark.csv")
//      .option("quote", "\"")
//      .option("header", value = true).save(outputDir)

    spark.stop()
  }
}
