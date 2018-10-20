package ArgoDataManagement


import ucar.nc2._

import collection.JavaConverters._

import Util.ArgoFloatException

class FloatData ( netcdf_path:String="src/main/resources/1900063_prof.nc") {



  def getJavaNetCDFObject:NetcdfFile = NetcdfFile.open(netcdf_path)

  def getVariables: Seq[Variable] = getJavaNetCDFObject.getVariables().asScala


  def getMap:Map[String,Array[_ >: Double with Int with Char with Float <: AnyVal]] = getVariables.map(file_var => {
    val key = caseConvert(file_var.getShortName)
    val value = file_var.read.copyTo1DJavaArray
    file_var.read.getElementType.toString match {
      case "double" => (key, value.asInstanceOf[Array[Double]])
      case "int" => (key, value.asInstanceOf[Array[Int]])
      case "char" => (key, value.asInstanceOf[Array[Char]])
      case "float" => (key, value.asInstanceOf[Array[Float]])
      case _ => throw new ArgoFloatException("\n   --- Required: [int || double || char || float] as variable datatypes from raw netCDF\n"+
          f"   +++ Found:\n   $file_var.read")
    }
  }).toMap

  // In future better with symbol or enum instead of string
  def getLongitude = getMap("longitude")


  // MACRO_CASE to camelCase
  private[this] def caseConvert(macroCaseString:String):String={
    val parts = macroCaseString.toLowerCase.split("_")

    val h = parts.head

    val t = parts.tail.map(part => {
      part.capitalize
    })
    val res = h +: t
    return res.mkString("")
  }
}
