import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * project: spark-demo
 * package: 
 * file:    SparkPartitionPivot
 * created: 2019-10-10
 * author:  rotem
 */
object SparkPartitionPivot extends BaseSparkApp {
  private def rand = scala.util.Random.nextInt(10) + 1

  private final val PARTITIONS = 4

  private final val C1: String = "c1"
  private final val C2: String = "c2"
  private final val C3: String = "c3"
  private final val MOD: String = "mod"

  private def floorMod: UserDefinedFunction = udf((x: Int, y: Int) => Math.floorMod(x, y))

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession(cores = 4, memory = 4)

    import spark.sqlContext.implicits._
    val df = spark.sparkContext.parallelize(Seq.fill(1000000)(rand, rand, rand))
      .toDF(C1, C2, C3)
      .withColumn(MOD, floorMod(col(C1), lit(PARTITIONS)))

    println(s"df has ${df.rdd.partitions.length} partitions")

    //    // open the browser on spark UI
    //    try {
    //      if (java.awt.Desktop.isDesktopSupported) {
    //        java.awt.Desktop.getDesktop.browse(new URI("http://localhost:4040"))
    //      }
    //    } catch {
    //      case _: Throwable =>
    //    }

    val schema = df.schema
    schema.printTreeString()

    df.show()

    val dynamicQuery = s"case when $C1 >= 5 and $C2 = 10 then 'a' else 'b' end"
    val df2 = df.withColumn("dyn", expr(s"$dynamicQuery"))
    df2.show()

    val partitionedDf = df.repartitionByRange(PARTITIONS, col(MOD))

    println(s"partitionedDf has ${partitionedDf.rdd.partitions.length} partitions")
    // perform pivot on 4 partitions
    val countDf = partitionedDf.groupBy(col(MOD))
      .pivot(col(MOD))
      .count()
      .na.fill(0)
      .sort(col(MOD))

    println(s"countDf has ${countDf.rdd.partitions.length} partitions")
    countDf.show()

    Thread.sleep(50000)
    spark.stop()
  }
}
