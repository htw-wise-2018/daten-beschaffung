package ArgoDataManagement

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import ucar.nc2._
import Util.ArgoFloatException
import collection.JavaConverters._

class BuoyData(netcdf_path: String = "src/main/resources/1900063_prof.nc") {

  def getJavaNetCDFObject: NetcdfFile = NetcdfFile.open(netcdf_path)

  def getVariables: Seq[Variable] = getJavaNetCDFObject.getVariables().asScala

  def getGlobalAttributes: Map[String, String] = {
    getJavaNetCDFObject.getGlobalAttributes().asScala
      .map(globAttr => (globAttr.getName, globAttr.getStringValue)).toMap
  }

  // MACRO_CASE to camelCase
  private[this] def caseConvert(macroCaseString: String): String = {
    val parts = macroCaseString.toLowerCase.split("_")

    val h = parts.head

    val t = parts.tail.map(part => {
      part.capitalize
    })
    val res = h +: t
    return res.mkString("")
  }
  
  // Cast list of float to list of double for mongodb
  private[this] def floatToDouble(in: List[Float]):List[Double]={
    val out = in.map(float =>  float.toDouble )
    return out
  }
  
  def getMap: Map[String, Array[_ >: Double with Int with String with Float]] = {
    getVariables
      .filter(file_var => file_var.getDimensions.get(0).getName == "N_PROF")
      .map(file_var => {
        val key = caseConvert(file_var.getShortName)
        val dims = file_var.getDimensions.asScala
        val value = file_var.read.copyTo1DJavaArray
        file_var.read.getElementType.toString match {
          case "double" => (key, value.asInstanceOf[Array[Double]])
          case "int"    => (key, value.asInstanceOf[Array[Int]])
          case "char" => {
            if (dims.size > 1) {
              (key, value.asInstanceOf[Array[Char]].grouped(dims(1).getLength).toArray.map(arrOfChar => arrOfChar.mkString.trim))
            } else {
              (key, value.asInstanceOf[Array[Char]].map(arrOfChar => {
                arrOfChar.toString()
              }))

            }
          }
          case "float" => (key, value.asInstanceOf[Array[Float]])
          case _ => throw new ArgoFloatException("\n   --- Required: [int || double || char || float] as variable datatypes from raw netCDF\n" +
            f"   +++ Found:\n   $file_var.read")
        }
      }).toMap
  }
  
  private[this] def preprocessData = {
    getVariables.
    filter(file_var => file_var.getDimensions.get(0).getName == "N_PROF").
    map(file_var => {
        val key = caseConvert(file_var.getShortName)
        val dims = file_var.getDimensions.asScala
        val value = file_var.read.copyToNDJavaArray
        file_var.read.getElementType.toString match {
          case "double" =>  {
              if (dims.size == 2) {
              (StructField(key, ArrayType(DoubleType,true), true), value.asInstanceOf[Array[Array[Double]]].toList)
              }else{
                 (StructField(key, DoubleType, true), value.asInstanceOf[Array[Double]].toList) 
              }
          }
          case "int"    => {
              if (dims.size == 2) {
              (StructField(key, ArrayType(IntegerType,true), true), value.asInstanceOf[Array[Array[Int]]].toList)
              }else{
                 (StructField(key, IntegerType, true), value.asInstanceOf[Array[Int]].toList) 
              }
          }
          case "char" => {
              if (dims.size == 4) {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Array[Array[Array[Char]]]]].map(arrOfChar => arrOfChar.mkString.trim).toList)
            } else if (dims.size == 3) {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Array[Array[Char]]]].map(arrOfChar => arrOfChar.mkString.trim).toList)
            } else if (dims.size == 2) {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Array[Char]]].map(arrOfChar => arrOfChar.mkString.trim).toList)
            } else {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Char]].map(arrOfChar => {
                arrOfChar.toString()
              }).toList)

            }
          }
          case "float" =>  {
              if (dims.size == 2) {
                  (StructField(key, ArrayType(DoubleType,true), true),
                      value.asInstanceOf[Array[Array[Float]]].
                      toList.map(floatList=>floatToDouble(floatList.toList)) ) 
              }else{
                 (StructField(key, DoubleType, true),floatToDouble(value.asInstanceOf[Array[Float]].toList)) 
              }
          }
          case _ => throw new ArgoFloatException("\n   --- Required: [int || double || char || float] as variable datatypes from raw netCDF\n" +
            f"   +++ Found:\n   $file_var.read")
        }
      }).toList.
        map(tuples => List(tuples._1, tuples._2)).
        transpose
        
  }
  
  def getRDD(sc: SparkContext):RDD[Row]={
    val groupedData = preprocessData(1).map(arr => arr.asInstanceOf[List[Any]]).
        transpose.
        map(arr => Row(arr:_*))
    sc.parallelize(groupedData)
  }
  
  private[this] def getSchema:StructType={
     StructType(preprocessData(0).asInstanceOf[List[StructField]])
  }
  
  def getDF(sc:SparkContext,sqlContext: SQLContext)={
    sqlContext.createDataFrame(getRDD(sc), getSchema)
  }
  // TODO In future better with symbol or enum instead of string
  def getLongitude = getMap("longitude")
  def getLatitude = getMap("latitude")

  
}


case class Buoy(longi: Double, lati: Double)
