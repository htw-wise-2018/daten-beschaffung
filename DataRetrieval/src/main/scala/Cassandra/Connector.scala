package Cassandra

;

import Preprocessing.ThisWeekList

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.SparkConf
import org.apache.log4j._

object Connector {


  /**
    * saves new data to cassandraDB
    *
    * @param data
    */
  def saveData(data: RDD[Row], sc: SparkContext): Unit = {

    val res = data.map(x => (x(1), x(0), x(2), x(3))).collect().toSeq


    //Saving data from RDD to Cassandra
    val collection = sc.parallelize(res)
    collection.saveToCassandra("ecco", "ocean_data", SomeColumns("file", "date", "latitude", "longitude"))

  }


  def loadData(sc: SparkContext): Unit = {

    //get the data from cassandra
    val res = sc.cassandraTable("ecco", "ocean_data")

    //print the result
    res.collect().map(println)
  }

  //[meds/4902465/profiles/R4902465_010.nc,20181019094100,54.453,-147.157,P,865,ME,20181019071545]
}
