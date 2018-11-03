package ArgoDataManagement

import ucar.nc2._


import Util.ArgoFloatException
import collection.JavaConverters._

class BuoyData ( netcdf_path:String="src/main/resources/1900063_prof.nc") {



  def getJavaNetCDFObject:NetcdfFile = NetcdfFile.open(netcdf_path)

  def getVariables: Seq[Variable] = getJavaNetCDFObject.getVariables().asScala

  def getGlobalAttributes: Map[String,String]={
    getJavaNetCDFObject.getGlobalAttributes().asScala
      .map(globAttr=>(globAttr.getName,globAttr.getStringValue)).toMap
  }


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

  // TODO In future better with symbol or enum instead of string
  def getLongitude = getMap("longitude")
  def getLatitude = getMap("latitude")


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

case class Buk(pres:Array[Float], date_creation:Array[Char], config_mission_number:Array[Int],
    scientific_calib_coefficient:Array[Char], temp_adjusted_error:Array[Float],
    parameter:Array[Char], juld_qc:Array[Char], data_mode:Array[Char],
    history_institution:Array[Char], pres_adjusted_qc:Array[Char], latitude:Array[Double],
    profile_pres_qc:Array[Char], psal:Array[Float], profile_psal_qc:Array[Char],
    positioning_system:Array[Char], format_version:Array[Char], cycle_number:Array[Int],
    position_qc:Array[Char], vertical_sampling_scheme:Array[Char], psal_adjusted:Array[Float],
    date_update:Array[Char], scientific_calib_comment:Array[Char], direction:Array[Char],
    pres_adjusted_error:Array[Float], psal_adjusted_error:Array[Float], data_state_indicator:Array[Char],
    platform_number:Array[Char], pres_adjusted:Array[Float], longitude:Array[Double],
    history_stop_pres:Array[Float], juld:Array[Double], temp_adjusted:Array[Float],
    platform_type:Array[Char], data_type:Array[Char], wmo_inst_type:Array[Char],
    temp_adjusted_qc:Array[Char], temp:Array[Float], history_action:Array[Char], handbook_version:Array[Char],
    history_qctest:Array[Char], scientific_calib_equation:Array[Char], history_step:Array[Char],
    history_parameter:Array[Char], history_software_release:Array[Char], pres_qc:Array[Char],
    history_previous_value:Array[Float], data_centre:Array[Char], history_date:Array[Char], profile_temp_qc:Array[Char],
    reference_date_time:Array[Char], scientific_calib_date:Array[Char], firmware_version:Array[Char], dc_reference:Array[Char],
    pi_name:Array[Char], station_parameters:Array[Char], history_reference:Array[Char], float_serial_no:Array[Char],
    psal_qc:Array[Char], history_start_pres:Array[Float], project_name:Array[Char], history_software:Array[Char],
    psal_adjusted_qc:Array[Char], temp_qc:Array[Char], juld_location:Double)


case class Buoy(longi: Double, lati: Double)
