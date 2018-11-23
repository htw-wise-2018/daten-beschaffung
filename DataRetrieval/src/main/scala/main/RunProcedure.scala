package main

import ArgoDataManagement.FloatData
import Preprocessing.ThisWeekList
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

// SciSpark imports
import org.dia.core.SciSparkContext
import Preprocessing.MongoController;

object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)


  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("HTW-Argo")

  // mongodb connect
  val host       = "127.0.0.1"
  val port       = 12345
  val db         = "db"
  val collection = "collection"
  val user       = "user"
  val password   = "password"

  val sc = new SparkContext(conf)
  val ssc = new SciSparkContext(sc)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") //database.collectionName
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll")
    .config(conf)
    .getOrCreate()



  def main(args: Array[String]) {


    // RUN DEMOS
    thisWeekListDemo
    localNetCDFtoRDDdemo
    floatDataDemo

    //save data to humongous
    saveData
    
    // Stop SparkSession
    spark.stop()
  }

  def saveData: Unit = {
    println("-------- START : saving this week list demo ---------")
    val float_list = new ThisWeekList(sc, spark.sqlContext)
    val float_list_rdd = float_list.toRDD

    MongoController.saveRDD(float_list_rdd, sc)

    println("-------- END : saving this week list demo ---------")
  }

  def floatDataDemo: Unit ={
    println("-------- START : Float data demo ---------")
    val fd = new FloatData
    println(s"Longitude array:\n[${fd.getLongitude.mkString(",")}]")
    println("-------- END : Float data demo ---------")
  }

  def thisWeekListDemo: Unit ={

    println("-------- START : This week list demo ---------")
    val float_list = new ThisWeekList(sc, spark.sqlContext)
    val float_list_df = float_list.toDF
    float_list_df.show                          // print DataFrame as formatted table
    val first_file = float_list_df.select("file").first.mkString
    println(s"First file:\n${first_file}")
    println("-------- END : This week list demo ---------")
  }

  def localNetCDFtoRDDdemo: Unit ={
    println("-------- START : Local NetCDF to RDD demo ---------")
    println("Local netCDF-file to RDD")
    val scientificRDD = ssc.netcdfFileList("src/main/resources/test_float.txt", List("PRES","LONGITUDE", "LATITUDE"))



    val arr = scientificRDD.take(1)(0).data
    println(arr.size)
    println("TOTAL "+arr.mkString(" "))
    println(arr.filter(l => l > -18.032).mkString(" "))
    println("-------- END : Local NetCDF to RDD demo ---------")
  }
}
