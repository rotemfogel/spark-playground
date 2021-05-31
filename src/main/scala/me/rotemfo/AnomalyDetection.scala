package me.rotemfo

import com.linkedin.relevance.isolationforest.IsolationForest
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col

object AnomalyDetection extends BaseSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val df = spark.read.parquet(Array(BASE_DIR, "user_page_views_fraud", "parquet").mkString("/"))
    df.show(truncate = false)


    val cols = df.columns
    val labelCol = cols.last

    val assembler = new VectorAssembler()
      .setInputCols(cols.slice(2, 2))
      .setOutputCol("features")
    val data = assembler
      .transform(df)
      .select(col("features"), col(labelCol).as("label"))

    // scala> data.printSchema
    // root
    //  |-- features: vector (nullable = true)
    //  |-- label: integer (nullable = true)

    /**
     * Train the model
     */

    val contamination = 0.1
    val isolationForest = new IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(false)
      .setMaxSamples(256)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(contamination)
      .setContaminationError(0.01 * contamination)
      .setRandomSeed(1)

    val isolationForestModel = isolationForest.fit(data)

    /**
     * Score the training data
     */

    val dataWithScores = isolationForestModel.transform(data)

    dataWithScores.printSchema
    spark.close()
  }
}
