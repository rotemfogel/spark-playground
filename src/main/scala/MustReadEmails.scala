import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * project: spark-playground
 * package: 
 * file:    MustReadEmails
 * created: 2019-12-16
 * author:  rotem
 */
object MustReadEmails extends BaseSparkApp {
  private final val schema: StructType = StructType(
    Array(
      StructField("user_id", StringType, nullable = false),
      StructField("year", IntegerType, nullable = false),
      StructField("created_at", TimestampType, nullable = false),
      StructField("last_seen", DateType, nullable = false)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()

    val path = "/home/rotem/Documents/sa/vertica/email/mustread_emails.csv"
    val mustRead = spark.read
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("inferSchema", value = false)
      .schema(schema)
      .csv(path)

    val outputDir = OUTPUT_DIR + "/user_agents"
    FileUtils.deleteQuietly(new File(outputDir))
    mustRead
      .repartition(col("year"))
      .select("user_id")
      .write
      .format("com.databricks.spark.csv")
      .option("header", value = true).save(outputDir)
    spark.stop()
  }
}
