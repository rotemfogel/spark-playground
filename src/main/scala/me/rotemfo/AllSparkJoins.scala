package me.rotemfo

import org.apache.spark.sql._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._

object AllSparkJoins {

  private val spark = SparkSession.builder().appName("AllSparkJoins").master("local").getOrCreate()
  private val sc = spark.sparkContext

  private val kids = sc.parallelize(
    Seq(
      Row(40, "Mary", 1),
      Row(41, "Jane", 3),
      Row(42, "David", 3),
      Row(43, "Angela", 2),
      Row(44, "Charlie", 1),
      Row(45, "Jimmie", 2),
      Row(46, "Lonely", 7)
    )
  )
  private val kidsSchema = StructType(
    Seq(
      StructField("Id", IntegerType),
      StructField("Name", StringType),
      StructField("Team", IntegerType)
    )
  )

  private val kidsDf = spark.createDataFrame(kids, kidsSchema)

  private val teams = sc.parallelize(
    Seq(
      Row(1, "The Invincibles"),
      Row(2, "Dog Lovers"),
      Row(3, "Rock Stars"),
      Row(4, "The Non-existent Team")
    )
  )

  private val teamsSchema = StructType(
    Seq(
      StructField("TeamId", IntegerType),
      StructField("TeamName", StringType)
    )
  )

  private val teamsDf = spark.createDataFrame(teams, teamsSchema)

  //--  joins -- //

  // inner join
  private val joinExpr: Column = kidsDf.col("Team") === teamsDf.col("TeamId")
  val kidTeamsDf: DataFrame = kidsDf.join(broadcast(teamsDf), joinExpr, "inner")

  // left outer join
  val kidsTeamsRightDf: DataFrame = kidsDf.join(broadcast(teamsDf), joinExpr, "left")
  // right  outer join
  val kidsTeamsLeftDf: DataFrame = kidsDf.join(broadcast(teamsDf), joinExpr, "right")
  // full outer join
  val fulKidTeamsDf: DataFrame = kidsDf.join(broadcast(teamsDf), joinExpr, "full")


  // semi join
  // SQL:
  // SELECT * FROM leftTable WHERE EXISTS (SELECT ...)
  val allKidsWithTeams: DataFrame = kidsDf.join(teamsDf, joinExpr, "left_semi")

  // anti join
  // SQL:
  // SELECT * FROM leftTable WHERE NOT EXISTS (SELECT ...)
  val kidsWithNoTeams: DataFrame = kidsDf.join(teamsDf, joinExpr, "left_anti")

  // cross join
  // cartesian product
  val productDf: DataFrame = kidsDf.crossJoin(teamsDf)

  def main(args: Array[String]): Unit = {
    kidTeamsDf.explain()
    kidTeamsDf.show(truncate = false)
    kidsTeamsLeftDf.show(truncate = false)
    kidsTeamsRightDf.show(truncate = false)
    fulKidTeamsDf.show(truncate = false)
    allKidsWithTeams.explain()
    allKidsWithTeams.show(truncate = false)
    kidsWithNoTeams.show(truncate = false)
    productDf.show(100, truncate = false)
    Thread.sleep(10000000)
  }
}
