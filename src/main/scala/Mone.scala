import java.io.File
import java.sql.Timestamp

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._

object Mone extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    try {
      //    import spark.sqlContext.implicits._
      //    val rdd = spark.sparkContext.textFile("/data/mone/2015/*.7z,/data/mone/2017/*.7z")
      //    val df = rdd.toDF()
      //    df.show

      val schema: StructType = StructType(
        Seq(StructField("instablog_id", IntegerType, nullable = false),
          StructField("pvs", LongType, nullable = false),
          StructField("created_at", TimestampType, nullable = false),
          StructField("updated_at", TimestampType, nullable = false)
        )
      )

      val df1 = spark.read
        .format("com.databricks.spark.csv")
        .option("header", value = false)
        .option("delimiter", "|")
        .schema(schema)
        .load("/home/rotem/dwh.instablog_pvs.tsv")

      df1.show()
      val a = df1.select("created_at").distinct().collect().map(_.getAs[Timestamp](0)).mkString(",")
      println(a)

      val outputDir = OUTPUT_DIR + "/instablogs_pvs/"
      FileUtils.deleteQuietly(new File(outputDir))

      df1.write.parquet(outputDir)

      val df2 = spark.read.parquet(outputDir)
      df2.show()
      val b = df2.select("created_at").distinct().collect().map(_.getAs[Timestamp](0)).mkString(",")

      assert(b.equals(a))

    } catch {
      case _: Throwable =>
    } finally {
      spark.close()
    }
  }
}
