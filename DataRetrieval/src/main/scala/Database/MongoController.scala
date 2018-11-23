package Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.bson.Document
import com.mongodb.spark._

object MongoController {


  /**
    * saves data RDD to Mongo
    * @param float_list_rdd
    * @param sc
    */
  def saveRDD(float_list_rdd: RDD[Row], sc: SparkContext) = {

    val res = float_list_rdd.map(x => (x(0), x(1), x(2), x(3))).map(x => Document.parse(s"{file: '${x._1}', date: '${x._2}', lat: '${x._3}', long: '${x._4}' }")).collect().toSeq
    val collection = sc.parallelize(res)

    MongoSpark.save(collection)

  }


}
