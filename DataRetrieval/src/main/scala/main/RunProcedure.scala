package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, Row, SQLContext}
import com.mongodb.spark._


import org.bson.Document
import org.bson.types.ObjectId


import database.DB
import ArgoDataManagement.BuoyData

object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("HTW-Argo")
    .set("spark.mongodb.output.uri","mongodb://127.0.0.1/ECCO.buoy")
    .set("spark.mongodb.input.uri","mongodb://127.0.0.1/ECCO.buoy?readPreference=primaryPreferred")
    .set("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]) {
    
    //dbDemo
    buoyDataDemoMongoDB
  }

  def dbDemo: Unit = {
    println("-------- START : mongodb demo ---------")
    val testDB = new DB
    val buoys = new BuoyData
    testDB.insertFirstBuoy(buoys)
    testDB.insertAllBuoys(buoys)
    testDB.close
    println("-------- END : mongodb demo ---------")
  }

  def buoyDataDemo: Unit = {
    println("-------- START : Buoy data demo ---------")
    val bd = new BuoyData
    println(s"Longitude array:\n[${bd.getLongitude.mkString(",")}]")
    println(bd.getGlobalAttributes)
    println(bd.getMap.keys)
    println(bd.getDF(sc, spark.sqlContext).show())
    println("-------- END : Buoy data demo ---------")
  }
  
  def buoyDataDemoMongoDB: Unit = {
    println("-------- START : Buoy data demo ---------")
    val bd = new BuoyData
    val bdDF = bd.getDF(sc, spark.sqlContext)
    println(bdDF.show())
    bdDF.select("floatSerialNo", "longitude", "latitude", "platformNumber", "projectName", "juld",
        "platformType", "configMissionNumber", "cycleNumber","pres","temp","psal").write.
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