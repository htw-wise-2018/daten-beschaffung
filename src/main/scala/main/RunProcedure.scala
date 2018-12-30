package main

import Database.MongoController
import main.RunProcedure.conf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import preprocessing.ThisWeekList
import netcdfhandling.BuoyData

import scala.concurrent.duration._
object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)

//    val hadoopUser = sys.env("HTW_MONGO_USER")
//    val hadoopPassword = sys.env("HTW_MONGO_PWD")
//    val hadoopDB = sys.env("HTW_MONGO_DB")
//    val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT","27020")
//    val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")
//
//    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("HTW-Argo")
//      .set("spark.mongodb.output.uri",s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy")
//      .set("spark.mongodb.input.uri",s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy?readPreference=primaryPreferred")

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("HTW-Argo")

    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL for Argo Data")
//      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") //database.collectionName
//      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll")
      .config(conf)
      .getOrCreate()



  def main(args: Array[String]) {
    thisWeekListDemo

  }


  def buoyDataDemo: Unit = {
    println("-------- START : Buoy data demo ---------")
    val bd = new BuoyData
    println(s"Longitude array:\n[${bd.getMap("longitude").mkString(",")}]")
    println(bd.getGlobalAttributes)
    println(bd.getMap.keys)
    println(bd.getDF(sc, spark.sqlContext).show())
    println("-------- END : Buoy data demo ---------")
  }

  def thisWeekListDemo: Unit = {

    println("-------- START : This week list demo ---------")
    val buoy_list = new ThisWeekList(sc, spark.sqlContext)
    val buoy_list_df = buoy_list.toDF

//    val bd = new BuoyData
//    val bdDF = bd.getDF(sc, spark.sqlContext)


    val mg = new MongoController(sc)
    mg.checkLastUpdate(buoy_list_df)

//    buoy_list_df.show // print DataFrame as formatted table
//    val first_file = buoy_list_df.select("file").first.mkString
//    println(s"First file:\n${first_file}")
    println("-------- END : This week list demo ---------")
  }

  def buoyDataDemoMongoDB: Unit = {

    println("-------- START : Buoy data demo ---------")

    val bd = new BuoyData
    val bdDF = bd.getDF(sc, spark.sqlContext)

    bdDF.select("floatSerialNo", "longitude", "latitude", "platformNumber", "projectName", "juld",
      "platformType", "configMissionNumber", "cycleNumber", "pres", "temp", "psal").write.
      format("com.mongodb.spark.sql.DefaultSource").mode("append").
      save()


    //val df2 = MongoSpark.load(spark)
    //println(df2.show())
    //val buoysFromDB = spark.read.
    //  format("com.mongodb.spark.sql.DefaultSource").
    //  load()
    //println(buoysFromDB.show())
    println("-------- END : Buoy data demo ---------")


  }

}
