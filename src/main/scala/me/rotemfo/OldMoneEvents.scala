package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//noinspection ScalaCustomHdfsFormat
object OldMoneEvents extends App {

  case class Year(prefix: String, year: Int, computeMax: Boolean = false)

  val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]").getOrCreate()

  def parseYear(y: Year): Unit = parseYear(y.prefix, y.year, y.computeMax)

  def parseYear(prefix: String, year: Int, computeMax: Boolean = false): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", value = false).option("delimiter", "\t").csv(s"/data/${prefix}_$year*.*")
    val (userIdCol, emailCol, tsCol) = year match {
      case y: Int if y < 2016 => ("_c36", "_c38", "_c2")
      case 2016 => ("_c33", "_c35", "_c2")
      case 2017 => ("_c26", "_c27", "_c2")
      case 2018 => ("_c28", "_c29", "_c2")
    }
    val df2 = df.where(col(userIdCol).isNotNull).select(userIdCol, emailCol)
    val df3 = if (computeMax) df2.withColumn("ts", to_timestamp(col(tsCol), "yyyy-MM-dd HH:mm:SS")).select(userIdCol, emailCol, "ts").groupBy(userIdCol, emailCol).agg(max("ts")) else df2.distinct
    df3.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", value = false).save(s"/data/output/$year")
  }

  Seq(
    Year("mone", 2012),
    Year("mone", 2013),
    Year("mone", 2014),
    Year("dw_mone", 2015),
    Year("dw_mone", 2016),
    Year("dw_mone", 2017),
    Year("dw_mone", 2018)
  ).foreach(parseYear)

}
