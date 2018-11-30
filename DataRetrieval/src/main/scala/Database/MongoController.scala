package Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.bson.Document
import com.mongodb.spark._
import akka.actor.Actor
import akka.actor.Props

import scala.concurrent.duration._
import com.mongodb.spark.config._
import main.RunProcedure.{sc, spark}
import com.mongodb.MongoClient

class MongoController(sc: SparkContext, float_list_rdd: RDD[Row]) {


  /**
    * get the latest update_date from mongoDB
    *
    * @return
    */
  def getLastUpdate: String = {

    val readConfig = ReadConfig(Map("collection" -> "lastupdate", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    if(!isEmpty(customRdd)){
      customRdd.map(x => x.getString("date")).first()
    } else {
      ""
    }

  }

  /**
    * get the latest update_date from the argo data server
    *
    * @return
    */
  def loadLatestData: String = {

    val thisWeek = new ThisWeekList(sc, spark.sqlContext)
    val dateLine = thisWeek.getUpdateDate
    val words = dateLine._1 replaceAll(" +", " ") split " " toList

    words.last

  }

  /**
    * check if an RDD is empty
    * @param rdd
    * @tparam T
    * @return
    */
  def isEmpty[T](rdd : RDD[T]) = {
    rdd.take(1).length == 0
  }

  /**
    * check if the data update date is changed, if so change it in the database
    */
  def checkLastUpdate: Unit = {

    val updateDate = getLastUpdate // get the update date from the db
    val lastUpdate = loadLatestData // get the update date from the server

    if (updateDate != lastUpdate) {

      //TODO- update the specific record and not add to the collection another record

      val writeConfig = WriteConfig(Map("collection" -> "lastupdate", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
      val sparkDocuments = sc.parallelize((1 to 1).map(i => Document.parse(s"{date: $lastUpdate}")))
      MongoSpark.save(sparkDocuments, writeConfig)

    }

  }


  /**
    * saves data RDD to Mongo
    *
    * @param float_list_rdd
    * @param sc
    */
  def saveRDD(float_list_rdd: RDD[Row], sc: SparkContext) = {

    val res = float_list_rdd.map(x => (x(0), x(1), x(2), x(3))).map(x => Document.parse(s"{file: '${x._1}', date: '${x._2}', lat: '${x._3}', long: '${x._4}' }")).collect().toSeq
    val collection = sc.parallelize(res)

    MongoSpark.save(collection)

  }


}
