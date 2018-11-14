package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import database.DB
import ArgoDataManagement.BuoyData

object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("HTW-Argo")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]) {
    //dbDemo
    buoyDataDemo
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

}