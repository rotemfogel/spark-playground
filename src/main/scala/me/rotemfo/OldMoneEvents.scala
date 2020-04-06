package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//noinspection ScalaCustomHdfsFormat
object OldMoneEvents extends App {

  case class Year(prefix: String, year: Int, computeMax: Boolean = false)

  val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]").getOrCreate()

  def parseYear(y: Year): Unit = parseYear(y.prefix, y.year, y.computeMax)

  def parseYear(prefix: String, year: Int, computeMax: Boolean = false): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", false).option("delimiter", "\t").csv(s"/data/${prefix}_${year}*.*")
    val (userIdCol, emailCol, tsCol) = year match {
      case y: Int if y < 2015 => ("_c36", "_c38", "_c2")
      case 2015 => ("_c32", "_c34", "_c2")
      case 2016 => ("_c33", "_c35", "_c2")
      case 2017 => ("_c26", "_c27", "_c2")
      case 2018 => ("_c28", "_c29", "_c2")
    }
    val whereDf = df.where(col(userIdCol).isNotNull)
    val writeDf = if (computeMax) {
      whereDf.withColumn("ts", to_timestamp(col(tsCol), "yyyy-MM-dd HH:mm:SS")).select(userIdCol, emailCol, "ts").groupBy(userIdCol, emailCol).agg(max("ts"))
    } else {
      whereDf.select(userIdCol, emailCol).distinct
    }
    writeDf.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", false).save(s"/data/output/${year}")
  }

  val schema = StructType(Seq(StructField("user_id", IntegerType, nullable = false), StructField("email", StringType, nullable = false)))

  def read(year: Int): DataFrame = {
    spark.read.format("com.databricks.spark.csv").option("header", false).option("delimiter", ",").schema(schema).csv(s"/data/output/$year/*.csv").withColumn("year_", lit(year))
  }

  val years = Seq(
    Year("mone", 2012),
    Year("mone", 2013),
    Year("mone", 2014),
    Year("dw_mone", 2015),
    Year("dw_mone", 2016),
    Year("dw_mone", 2017),
    Year("dw_mone", 2018)
  )
  years.foreach(parseYear)

  val df = read(2012).union(read(2013)).union(read(2014)).union(read(2015)).union(read(2016))
  val agg = df.select("user_id", "email").distinct()
  agg.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", false).save("/data/output/final")

}
