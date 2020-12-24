package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Events extends BaseSparkApp {
  val extractUserId: Column = get_json_object(col("data"), "$.user_id")
  val columnsFnMap: Map[String, Column] = Map(
    "user_id" -> extractUserId,
    "new_user_id" -> extractUserId.plus(lit(10)),
    "url_first_level" -> split(col("url"), "/").getItem(1)
  )

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    import spark.implicits._
    val df = Seq(
      ("/account/portfolio/summary", "{\"user_id\":1}"),
      ("/news/3647269", "{\"user_id\":2}"),
      ("/symbol/SNOW", "{\"user_id\":3}")
    ).toDF("url", "data")
    df.show(truncate = false)
    val df2 = columnsFnMap.foldLeft(df) { case (d, m) => d.withColumn(m._1, m._2) }.drop("url", "data")
    df2.show(truncate = false)
  }
}
