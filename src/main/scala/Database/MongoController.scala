package Database

import sys.process._
import java.net.URL
import java.io.File

import com.mongodb.client.model.{Filters, Projections}
import main.RunProcedure.sc
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.mongodb.scala.result.UpdateResult
//import org.bson.Document
import com.mongodb.spark._
import com.mongodb.spark.config._
import main.RunProcedure.{sc, spark}
import netcdfhandling.BuoyData
import preprocessing.ThisWeekList

import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import java.io._
import java.math.BigInteger


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import ucar.nc2._
import eccoutil.ArgoFloatException
import collection.JavaConverters._
import Database.Helpers
import org.mongodb.scala.model.Updates._
import org.mongodb.scala._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.Filters._


object MongoController {


  // Use a Connection String
  val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017")
  val database: MongoDatabase = mongoClient.getDatabase("test")
  val updateTimeCollection: MongoCollection[Document] = database.getCollection("lastupdate")
  val dataCollection: MongoCollection[Document] = database.getCollection("coll")


  val downloadsPath = "src/main/resources/Downloads/"


  /**
    * get the latest netcdf data from the server and "add them" to the database
    */
  def saveLatestData: Unit = {


    val buoy_list = new ThisWeekList(sc, spark.sqlContext)
    val buoydf = buoy_list.toDF.rdd

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

      //get the doc from mongo
      val updateDate = dataCollection.find()
        .projection(Projections.fields(Projections.include("floatSerialNo")))
        .first()

      val isEmpty = Helpers.DocumentObservable(updateDate).results().size

      println(isEmpty)

      if (isEmpty != 0) {

        //TODO- update the  boy to the humongous

      } else {

        //TODO- insert the  boy to the humongous

      }

      // delete the source file
      new File(downloadPath + documentName).delete()


    }).collect()

  }


  /**
    * get the latest update_date from file
    *
    * @return
    */
  def getLastUpdate: String = {

    //get the doc from mongo
    val updateDate = updateTimeCollection.find()
      .projection(Projections.fields(Projections.include("date")))
      .first()

    val isEmpty = Helpers.DocumentObservable(updateDate).results().size

    if (isEmpty != 0) {

      Helpers.DocumentObservable(updateDate).results().map(d => d.getString("date")).head

    } else {
      ""
    }
  }

  /**
    * get the latest update_date from the argo data server
    *
    * @return
    */
  def loadLatestUpdateDate: String = {

    val thisWeek = new ThisWeekList(sc, spark.sqlContext)
    val dateLine = thisWeek.getUpdateDate

    val words = dateLine._1 replaceAll(" +", " ") split " " toList

    words.last

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


    val updateDateFromMongo = getLastUpdate // get the update date from the db
    val lastUpdateDateFromServer = loadLatestUpdateDate // get the update date from the server

        if (updateDateFromMongo != lastUpdateDateFromServer) {


          if (getLastUpdate.isEmpty) {

            //insert date
            val doc: Document = Document("_id" -> 1,
              "name" -> "Last Date of Buoys Update",
              "date" -> lastUpdateDateFromServer)

            updateTimeCollection.insertOne(doc)
              .subscribe(new Observer[Completed] {
                override def onNext(result: Completed): Unit = println("Inserted")

                override def onError(e: Throwable): Unit = println("Failed")

                override def onComplete(): Unit = println("Completed")
              })

          } else {

            // update the new date to the humongous
            val x = updateTimeCollection.updateOne(equal("_id", 1), set("date", lastUpdateDateFromServer))
            Helpers.GenericObservable(x).printHeadResult()

          }

          saveLatestData

        }



  }


}

