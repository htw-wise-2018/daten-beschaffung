package main

import ArgoDataManagement.FloatData


object RunProcedure {


  def main(args: Array[String]) {


    // RUN DEMO
    floatDataDemo

  }

  def floatDataDemo: Unit ={
    println("-------- START : Float data demo ---------")
    val fd = new FloatData
    println(s"Longitude array:\n[${fd.getLongitude.mkString(",")}]")
    println("-------- END : Float data demo ---------")
  }



}
