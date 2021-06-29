package me.rotemfo

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import org.apache.spark.sql.functions.{col, lit, to_json}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.sql.Date
import java.time.LocalDate

object Stats extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    //    val schema = StructType(
    //      Seq(
    //        StructField("date_", DateType, nullable = false),
    //        StructField("user_id", IntegerType, nullable = false),
    //        StructField("subscription_product_id_group", StringType, nullable = false),
    //        StructField("pvs", LongType, nullable = false)
    //      )
    //    )
    val baseDir = "/data/user_pvs"

    def save(df: DataFrame, d: Option[Date] = None): Unit = {
      val filtered = if (d.isDefined) df.where(col("date_") === lit(d.get)) else df
      val analysisResult: AnalyzerContext = {
        AnalysisRunner
          // data to run the analysis on
          .onData(filtered)
          // define analyzers that compute metrics
          .addAnalyzer(Mean("pvs"))
          .addAnalyzer(StandardDeviation("pvs"))
          .addAnalyzer(Maximum("pvs"))
          .addAnalyzer(Minimum("pvs"))
          .run()
      }

      val date = if (d.isDefined) d.get else Date.valueOf(LocalDate.of(2999, 12, 31))

      // retrieve successfully computed metrics as a Spark data frame
      val metrics = successMetricsAsDataFrame(spark, analysisResult)
      metrics
        .withColumn("date_", lit(date))
        .select("date_", "name", "value")
        .repartition(1)
        .write
        .format("csv")
        .option("delimiter", "|")
        .mode(SaveMode.Overwrite)
        .save(s"$baseDir/csv/$date")
      //        .format("jdbc")
      //        .option("url", "jdbc:mysql://127.0.0.1:3306/seekingalpha")
      //        .option("dbtable", "seekingalpha.pv_metrics")
      //        .option("user", "seeking")
      //        .option("password", "alpha")
      //        .mode(SaveMode.Append) // <--- Append to the existing table
      //        .save()
    }

    val df = spark.read.parquet(s"$baseDir/parquet/*").persist(StorageLevel.MEMORY_AND_DISK)
    df.withColumn("product_ids", to_json(col("product_id")))
      .drop("product_id")
      .withColumnRenamed("product_ids", "product_id")
      .repartition(1)
      .write
      .format("csv")
      .option("delimiter", "|")
      .mode(SaveMode.Overwrite)
      .save(s"$baseDir/csv/full/")

    val dates: Seq[Date] = df.select("date_").distinct.collect().map(d => d.getDate(0))

    implicit def ordered: Ordering[Date] = new Ordering[Date] {
      def compare(x: Date, y: Date): Int = x.compareTo(y)
    }

    dates.sorted.foreach(d => save(df, Some(d)))
    save(df)
  }
}