package preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/** Class to retrieve and process information / index of the 'new' (in weekly time periods) uploaded argo-data
  *
  * The input file contains following data for each of the new uploaded NetCDF files:
  * - file : used to download only the current files
  * - date
  * - latitude
  * - longitude
  * - ocean
  * - profiler_type
  * - institution
  * - date_update
  *
  * And the input file also contains some meta data (the meta data rows are at the beginning and start with '#')
  * - Title
  * - Description
  * - Project
  * - Format version
  * - Date of update : could be used to check if the download process has to be triggered again
  * - FTP root number 1 : used to retrieve the full path with the path specified in the file field
  * - FTP root number 2
  * - GDAC node
  *
  * @param sc Current SparkContext
  * @param sqlContext Current SqlContext
  * @param path Location of the remote txt-file containing the information
  * @param username FTP user (currently not required)
  * @param password FTP password (currently not required)
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de
  * @see See [[https://github.com/htw-wise-2018]] for more information.
  */
class ThisWeekList(sc: SparkContext,
                   sqlContext: SQLContext,
                   path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt",
                   username: String = "anonymous",
                   password: String = "empty") {


  /** Returns fullpath for this objects txt file.
    */
  private[this] def fullpath: String = s"ftp://$username:$password@$path"

  /** Returns this objects txt file as String.
    */
  private[this] def fetch_txt: () => String = {
    val fetched_txt = sc.wholeTextFiles(fullpath)
    val text_string = fetched_txt.take(1)(0)._2
    () => text_string
  }

  /** Returns cached txt file as String.
    */
  private[this] val fullString: () => String = fetch_txt
  /** Returns this objects txt file as Array of lines.
    */
  private[this] val rowArray: Array[String] = fullString().split("\n")


  /** Returns an inferred schema from this objects txt file. Meta data NOT included.
    */
  private[this] def schema_string: (String, Int) = {
    rowArray.zipWithIndex.view.filter(row => row._1.charAt(0) != '#').head
  }

  /** Returns a schema array of this objects txt file, used to create a Spark StructType.
    */
  val schema_array: Array[String] = schema_string._1.split(",")
  /** Returns a Spark StructType, used to create a Spark DataFrame.
    */
  private[this] val schema: StructType = {
    StructType(schema_array.map(fieldName => StructField(fieldName, StringType, true)))
  }

  /** Returns an Scala Array from this objects txt file.
    */
  private[this] def toArray: Array[Array[String]] = {
    val without_header = rowArray.drop(schema_string._2 + 1)
    without_header.map(row => row.split(","))
  }


  /** Returns update date of this objects txt file.
    */
  def getUpdateDate: (String, Int) = {
    rowArray.zipWithIndex.view.filter(row => row._1.charAt(0) == '#' && row._1.contains("Date of update")).head
  }

  /** Returns ftp-root specified as meta data in this objects txt file.
    */
  def getRootFTP: (String)={
    rowArray.view.filter(row => row.charAt(0) == '#' && row.contains("FTP root number 1")).head.split(": ")(1).trim
  }

  /** Returns a Spark RDD from this objects txt file.
    */
  def toRDD: RDD[Row] = sc.parallelize(toArray).map(row => Row.fromSeq(row))

  /** Returns a Spark DataFrame from this objects txt file.
    */
  def toDF: DataFrame = sqlContext.createDataFrame(toRDD, schema)

}