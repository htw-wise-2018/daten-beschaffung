package main

import database.DB
import ArgoDataManagement.BuoyData


object RunProcedure {

  def main(args: Array[String]) {
    dbDemo
  }
  
  def dbDemo: Unit = {
    println("-------- START : mongodb demo ---------")
    val testDB = new DB
    val buoys = new BuoyData
    //testDB.insertFirstBuoy(buoys)
    testDB.insertAllBuoys(buoys)
    testDB.close
    println("-------- END : mongodb demo ---------")
  }

  def buoyDataDemo: Unit = {
    println("-------- START : Buoy data demo ---------")
    val bd = new BuoyData
    println(s"Longitude array:\n[${bd.getLongitude.mkString(",")}]")
    println(bd.getGlobalAttributes)
    println("-------- END : Buoy data demo ---------")
  }

}