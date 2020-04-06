package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//noinspection ScalaCustomHdfsFormat
object OldMoneEvents extends App {

  case class ParseYear(prefix: String, year: Int, computeMax: Boolean = false)

  val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]").getOrCreate()

  val emailCol = "email"
  val userIdCol = "user_id"
  val tsCol = "ts"

  def read(prefix: String, year: Int, computeMax: Boolean): DataFrame = {
    val (tmpUserIdCol, tmpEmailCol, tmpTsCol) = year match {
      case y: Int if y < 2015 => ("_c36", "_c38", "_c1")
      case y: Int if y >= 2015 && y <= 2017 => ("_c32", "_c34", "_c1")
      case 2018 => ("_c27", "_c28", "_c2")
    }
    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", false)
      .option("delimiter", "\t")
      .csv(s"/data/${prefix}_${year}*.*")
      .where(col(tmpUserIdCol).isNotNull)
      .withColumnRenamed(tmpEmailCol, emailCol)
      .withColumnRenamed(tmpUserIdCol, userIdCol)
    if (computeMax) {
      df.withColumn(tsCol, to_timestamp(col(tmpTsCol), "yyyy-MM-dd HH:mm:SS")).select(userIdCol, emailCol, tsCol).groupBy(userIdCol, emailCol).agg(max(tsCol))
    } else {
      df.select(userIdCol, emailCol).distinct
    }
  }

  def parseYear(prefix: String, year: Int, computeMax: Boolean = false): Unit = {
    val df = read(prefix, year, computeMax)
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", false).save(s"/data/output/${year}")
    df.unpersist
  }

  def parse(y: ParseYear): Unit = parseYear(y.prefix, y.year, y.computeMax)

  val schema = StructType(Seq(StructField("user_id", IntegerType, nullable = false), StructField("email", StringType, nullable = false)))

  val years = Seq(
    ParseYear("mone", 2012, computeMax = true),
    ParseYear("mone", 2013, computeMax = true),
    ParseYear("mone", 2014, computeMax = true),
    ParseYear("dw_mone", 2015, computeMax = true),
    ParseYear("dw_mone", 2016, computeMax = true),
    ParseYear("dw_mone", 2017, computeMax = true),
    ParseYear("dw_mone", 2018, computeMax = true)
  )
  years.foreach(parse)

  //  val df = read(2012).union(read(2013)).union(read(2014)).union(read(2015)).union(read(2016))
  //  val agg = df.select("user_id", "email").distinct()
  //  agg.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", false).save("/data/output/final")

}
