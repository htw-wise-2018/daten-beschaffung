package netcdfhandling

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import ucar.nc2._
import eccoutil.ArgoFloatException
import collection.JavaConverters._
import java.net._

/** Class for argo-data from specified NetCDF.
  *
  * Usage:
  * {{{
  * val bd = new BuoyData
  * println(s"Longitude array:\n[${bd.getMap("longitude").mkString(",")}]")
  * println(bd.getGlobalAttributes)
  * println(bd.getMap.keys)
  * println(bd.getDF(sc, spark.sqlContext).show())
  * }}}
  *
  * @constructor Creates a object to process data from a NetCDF file.
  * @param netcdf_path URI to NetCDF file, also supports remote path e.g. FTP server.
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de Contact
  * @see See [[https://github.com/htw-wise-2018]] for more information.
  */
class BuoyData(netcdf_path: String = "src/main/resources/20180606_prof.nc") {
  /** Returns a Java NetCDF object from this objects NetCDF path and opens it into memory.
    */
  def getJavaNetCDFObject: NetcdfFile = NetcdfFile.openInMemory(new URI(netcdf_path))

  /** Returns argo-variables from this objects NetCDF file as Scala Sequence.
    */
  def getVariables: Seq[Variable] = getJavaNetCDFObject.getVariables().asScala

  /** Returns argo-globalattributes from this objects NetCDF file as Scala Map
    */
  def getGlobalAttributes: Map[String, String] = {
    getJavaNetCDFObject.getGlobalAttributes().asScala
      .map(globAttr => (globAttr.getName, globAttr.getStringValue)).toMap
  }

  /** Returns camelCase String from MACRO_CASE String.
    */
  private[this] def caseConvert(macroCaseString: String): String = {
    val parts = macroCaseString.toLowerCase.split("_")

    val h = parts.head

    val t = parts.tail.map(part => {
      part.capitalize
    })
    val res = h +: t
    return res.mkString("")
  }

  /** Typecast a list of float to a list of double for mongodb.
    */
  private[this] def floatToDouble(in: List[Float]): List[Double] = {
    val out = in.map(float => float.toDouble)
    return out
  }

  /** Returns a Scala Map of argo-variables from this objects NetCDF file.
    *
    * @throws ArgoFloatException
    */
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
          case _ => throw new ArgoFloatException("\n   --- Required: [int || double || char || float] as variable datatypes from raw netCDF\n+++ Found:\n   "+file_var.read)
        }
      }).toMap
  }

  /** Returns argo-variables as Scala List of Lists, used to create Spark RDD / Spark DataFrame.
    *
    * @return The first List contains the names of the argo-variables.
    *   The second List contains the values of the argo-variables.
    */
  private[this] def preprocessData = {
    getVariables.
      filter(file_var => file_var.getDimensions.get(0).getName == "N_PROF").
      map(file_var => {
        val key = caseConvert(file_var.getShortName)
        val dims = file_var.getDimensions.asScala
        val value = file_var.read.copyToNDJavaArray
        file_var.read.getElementType.toString match {
          case "double" => {
            if (dims.size == 2) {
              (StructField(key, ArrayType(DoubleType, true), true), value.asInstanceOf[Array[Array[Double]]].toList)
            } else {
              (StructField(key, DoubleType, true), value.asInstanceOf[Array[Double]].toList)
            }
          }
          case "int" => {
            if (dims.size == 2) {
              (StructField(key, ArrayType(IntegerType, true), true), value.asInstanceOf[Array[Array[Int]]].toList)
            } else {
              (StructField(key, IntegerType, true), value.asInstanceOf[Array[Int]].toList)
            }
          }
          case "char" => {
            if (dims.size == 4) {
              (StructField(key, ArrayType(ArrayType(StringType, true), true), true), value.asInstanceOf[Array[Array[Array[Array[Char]]]]]
                .map(arrOfChars => arrOfChars
                  .map(arrayOfChar => arrayOfChar
                    .map(arrayOfCha => arrayOfCha.mkString.trim).toList).toList).toList)
            
            } else if (dims.size == 3) {

              (StructField(key, ArrayType(StringType, true), true), value.asInstanceOf[Array[Array[Array[Char]]]].map(arrOfChars => arrOfChars.map(arrayOfChar => arrayOfChar.mkString.trim).toList).toList)
            } else if (dims.size == 2) {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Array[Char]]].map(arrOfChar => arrOfChar.mkString.trim).toList)
            } else {
              (StructField(key, StringType, true), value.asInstanceOf[Array[Char]].map(arrOfChar => {
                arrOfChar.toString()
              }).toList)

            }
          }
          case "float" => {
            if (dims.size == 2) {
              (
                StructField(key, ArrayType(DoubleType, true), true),
                value.asInstanceOf[Array[Array[Float]]].
                toList.map(floatList => floatToDouble(floatList.toList)))
            } else {
              (StructField(key, DoubleType, true), floatToDouble(value.asInstanceOf[Array[Float]].toList))
            }
          }
          case _ => throw new ArgoFloatException("\n   --- Required: [int || double || char || float] as variable datatypes from raw netCDF\n+++ Found:\n"+file_var.read)
        }
      }).toList.
      map(tuples => List(tuples._1, tuples._2)).
      transpose

  }

  /** Returns a Spark RDD from this objects NetCDF file.
    *
    * @param sc Current SparkContext
    */
  def getRDD(sc: SparkContext): RDD[Row] = {
    val groupedData = preprocessData(1).map(arr => arr.asInstanceOf[List[Any]]).
      transpose.
      map(arr => Row(arr: _*))
    sc.parallelize(groupedData)
  }

  /** Returns a Spark StructType, used to create a Spark DataFrame.
    */
  private[this] def getSchema: StructType = {
    StructType(preprocessData(0).asInstanceOf[List[StructField]])
  }

  /** Returns a Spark DataFrame from this objects NetCDF file.
    *
    * @param sc Current SparkContext
    * @param sqlContext Current SqlContext
    */
  def getDF(sc: SparkContext, sqlContext: SQLContext) = {
    sqlContext.createDataFrame(getRDD(sc), getSchema)
  }

}

