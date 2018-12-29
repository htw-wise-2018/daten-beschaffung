package Database

import sys.process._
import java.net.URL
import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.bson.Document
import com.mongodb.spark._
import com.mongodb.spark.config._
import main.RunProcedure.{sc, spark}
import netcdfhandling.BuoyData
import preprocessing.ThisWeekList

import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import java.io._
import java.math.BigInteger

class MongoController(sc: SparkContext) {

  @transient
  val downloadsPath = "src/main/resources/Downloads/"


  /**
    * get the latest netcdf data from the server and "add them" to the database
    */
  def saveLatestData(buoydf: RDD[Row]): Unit = {

    //path where the files will be saved from the server
    val downloadPath = this.downloadsPath

    //if the Download directory doesnt exist create it
    val scc = sc.parallelize(buoydf.collect)

    scc.map(x => {

      val path = x(0).toString
      // Split path into segments
      val segments = path.split("/")
      // Grab the last segment
      val documentName = segments(segments.length - 1)


      //get the source file from the server
      new URL("ftp://ftp.ifremer.fr/ifremer/argo/dac/" + path) #> new File(downloadPath + documentName) !!


      val bd = new BuoyData(downloadPath + documentName)

      println(s"Longitude array:\n[${bd.getMap("longitude").mkString(",")}]")

      //save the data to mongodb
      //      val bdDF = bd.getDF(sc, spark.sqlContext)
      //      println(bdDF.show())
      //      bdDF.select("floatSerialNo", "longitude", "latitude", "platformNumber", "projectName", "juld",
      //        "platformType", "configMissionNumber", "cycleNumber", "pres", "temp", "psal").write.
      //        format("com.mongodb.spark.sql.DefaultSource").mode("append").
      //        save()

      // delete the source file
      new File(downloadPath + documentName).delete()


    }).collect()


  }


  /**
    * get the latest update_date from file
    *
    * @return
    */
  def getLastUpdate: BigInt = {

    val readConfig = ReadConfig(Map("collection" -> "lastupdate", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)

    if (customRdd.count() != 0) {

      val d = customRdd.map(x => x.getLong("date")).first()
      BigInt(d)

    } else {
      BigInt(0)
    }


  }

  /**
    * get the latest update_date from the argo data server
    *
    * @return
    */
  def loadLatestUpdateDate: BigInt = {

    val thisWeek = new ThisWeekList(sc, spark.sqlContext)
    val dateLine = thisWeek.getUpdateDate

    val words = dateLine._1 replaceAll(" +", " ") split " " toList

    BigInt(words.last)

  }

  /**
    * check if an RDD is empty
    *
    * @param rdd
    * @tparam T
    * @return
    */
  def isEmpty[T](rdd: RDD[T]) = {
    rdd.take(1).length == 0
  }

  /**
    * check if the data update date is changed, if so change it in the database
    */
  def checkLastUpdate: Unit = {

    val updateDate = getLastUpdate // get the update date from the db
    val lastUpdate = loadLatestUpdateDate // get the update date from the server





    if (updateDate != lastUpdate) {

      //save the new date to the Database
      val writeConfig = WriteConfig(Map("collection" -> "lastupdate", "writeConcern.w" -> "majority", "replaceDocument"->"true"), Some(WriteConfig(sc)))
      val sparkDocuments = sc.parallelize((1 to 1).map(i => Document.parse(s"{date: $lastUpdate}")))
      MongoSpark.save(sparkDocuments, writeConfig)

      //save the new data to humongous
      //      val buoy_list = new ThisWeekList(sc, spark.sqlContext)
      //      val buoy_list_df = buoy_list.toDF.rdd
      //      saveLatestData(buoy_list_df)

    }

  }


}

