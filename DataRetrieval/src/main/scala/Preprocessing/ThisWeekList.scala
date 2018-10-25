package Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class ThisWeekList( sc: SparkContext,
                    sqlContext: SQLContext,
                    path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt",
                    username:String="anonymous",
                    password:String = "empty" ) {



  private[this] def fullpath:String = s"ftp://$username:$password@$path"

  private[this] def fetch_txt: () => String = {
    val fetched_txt = sc.wholeTextFiles(fullpath)
    val text_string = fetched_txt.take(1)(0)._2
    () => text_string
  }
  private[this] val fullString: () => String = fetch_txt
  private[this] val rowArray: Array[String] = fullString().split("\n")


  // EXTRACT SCHEMA
  private[this] def schema_string: (String, Int) = {
    rowArray.zipWithIndex.view.filter(row => row._1.charAt(0) != '#').head
  }
  val schema_array:Array[String] = schema_string._1.split(",")
  private[this] val schema:StructType = {
    StructType( schema_array.map(fieldName => StructField(fieldName, StringType, true)))
  }

  private[this] def toArray: Array[Array[String]] = {
    val without_header = rowArray.drop(schema_string._2+1)

    without_header.map(row=>row.split(","))
  }

  def toRDD: RDD[Row] = sc.parallelize(toArray).map(row => Row.fromSeq(row))


  def toDF: DataFrame = sqlContext.createDataFrame(toRDD, schema)
}
