import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/**
 * project: spark-playground
 * package: 
 * file:    SparkTest
 * created: 2019-11-27
 * author:  rotem
 */
object SparkTest extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession(cores = 4, memory = 8)

    val csv: DataFrame = spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/home/rotem/content.csv")

    import UdfStore.calcLagPercent
    import org.apache.spark.sql.functions._

    val window = Window.partitionBy(col("content_id")).orderBy(col("datediff"))
    val windowDF = csv.withColumn(colNameLag, lag(col("count"), 1) over window)
    // val selection = windowDF.select("content_id", "count", "datediff").groupBy("content_id", "count").min("datediff")

    val xxx = windowDF
      .withColumn("diff", calcLagPercent(col("count"), col("lag")))
      .withColumn("pct", when(col("diff").isNull, lit(0)).otherwise(col("diff")))
      .drop(col("diff"))

    val a = xxx.filter("datediff >= 0").groupBy(col("datediff")).avg("pct")
    a.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/home/rotem/content2/")
    spark.close()
  }
}
