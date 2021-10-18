//package me.rotemfo
//
//import com.seekingalpha.contract.Event
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.{Dataset, Encoder, Encoders}
//
//object MoneV2 extends BaseSparkApp with App {
//  val spark = getSparkSession()
//  //  val encoder = ExpressionEncoder[Event]
//  val encoder: Encoder[Event] = Encoders.kryo(classOf[Event])
//  val exprEncoder = encoder.asInstanceOf[ExpressionEncoder[Event]]
//  val df: Dataset[Event] = spark.read.json("/data/mone_v2/2021/06/01/05/*.gz").as(exprEncoder)
//  df.printSchema()
//}
