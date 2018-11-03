package database

import ArgoDataManagement.BuoyData

import org.mongodb.scala.{
  MongoClient,
  MongoCollection,
  MongoDatabase,
  Document,
  Observable,
  Completed,
  Observer
}

import scala.concurrent._
import scala.concurrent.duration._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros
import org.bson.codecs.configuration.CodecRegistries.{ fromRegistries, fromProviders }

case class Buoy(longi: Double, lati: Double)

class DB(name: String = "ECCO", collection: String = "buoy") {
  // SETUP
  private[this] val personCodecProvider = Macros.createCodecProvider[Buoy]()
  private[this] val codecRegistry = fromRegistries(fromProviders(personCodecProvider), DEFAULT_CODEC_REGISTRY)
  private[this] val mc = MongoClient() // TODO
  private[this] val mdb: MongoDatabase = mc.getDatabase(name).withCodecRegistry(codecRegistry)
  val dbBuoy: MongoCollection[Buoy] = mdb.getCollection(collection);

  def insertFirstBuoy(buoys: BuoyData): Unit = { 
    val fMap = buoys.getMap //.mapValues(arr => new BsonArray(new BsonString(arr.toList))
    val buoy = Buoy(fMap("longitude")(0).asInstanceOf[Double], fMap("latitude")(0).asInstanceOf[Double])
    val insertObservable: Observable[Completed] = dbBuoy.insertOne(buoy)

    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")
      override def onError(e: Throwable): Unit = println(s"onError: $e")
      override def onComplete(): Unit = println("onComplete")
    })
    val insertFuture = insertObservable.toFuture()
    val waitDuration = Duration(15, "seconds")
    Await.result(insertFuture, waitDuration)
  }
  
  
   def insertAllBuoys(buoys: BuoyData): Unit = { 

    val pairedGeoCords = buoys.getLongitude.asInstanceOf[Array[Double]].zip(buoys.getLatitude.asInstanceOf[Array[Double]])
    val allBuoys = pairedGeoCords.map(geocords => Buoy(geocords._1,geocords._2))
    
    val insertObservable: Observable[Completed] = dbBuoy.insertMany(allBuoys)

    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")
      override def onError(e: Throwable): Unit = println(s"onError: $e")
      override def onComplete(): Unit = println("onComplete")
    })
    val insertFuture = insertObservable.toFuture()
    val waitDuration = Duration(15, "seconds")
    Await.result(insertFuture, waitDuration)
  }
  
  def close:Unit={
    mc.close()
  }
}