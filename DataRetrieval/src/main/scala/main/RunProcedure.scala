package main

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SparkSession }
import preprocessing.ThisWeekList
import netcdfhandling.BuoyData


/** Main object to run argo-data retrieval.
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de
  */
object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)

  // Use environment variables for authentication
  val hadoopUser = sys.env("HTW_MONGO_USER")
  val hadoopPassword = sys.env("HTW_MONGO_PWD")
  val hadoopDB = sys.env("HTW_MONGO_DB")
  val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")

  // Basic Spark configuration. Use 'buoy' as mongodb collection.
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("HTW-Argo")
    .set("spark.mongodb.output.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy")
    .set("spark.mongodb.input.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy?readPreference=primaryPreferred")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]) {
    saveAllFromThisWeekList
    sc.stop()
    spark.stop()
  }

  /** Download the new argo data from: ftp://ftp.ifremer.fr/ifremer/argo/
    *
    * 1. Retrieve index of new NetCDF files from: ftp://ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt
    * 2. Foreach new file run [[saveDataMongoDB]]
    *
    */
  def saveAllFromThisWeekList: Unit = {
    val buoy_list = new ThisWeekList(sc, spark.sqlContext)
    val rootFTP = buoy_list.getRootFTP
    val weeklist= buoy_list.toRDD.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    weeklist.foreach(saveDataMongoDB)
  }

  /** Store argo-data in mongodb of one specific NetCDF file.
    *
    * @param filename NetCDF file path.
    */
  def saveDataMongoDB(filename: String): Unit = {
    val bd = new BuoyData(filename)
    val bdDF = bd.getDF(sc, spark.sqlContext)
    val extractCurrentParams = bdDF.select("parameter").first.get(0).asInstanceOf[Seq[Seq[String]]].flatten
    val selectColumns = extractCurrentParams ++ Seq("floatSerialNo", "longitude", "latitude", "platformNumber", "projectName", "juld",
      "platformType", "configMissionNumber", "cycleNumber")
    bdDF.select(selectColumns.head, selectColumns.tail: _*).write.
      format("com.mongodb.spark.sql.DefaultSource").mode("append").
      save()
  }

}
